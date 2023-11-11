/*
Copyright 2022

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package tiingo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/jackc/pgx/v4"
	"github.com/penny-vault/import-tiingo/common"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/ratelimit"
)

type TiingoApi struct {
	token string
	rate  ratelimit.Limiter
}

type Eod struct {
	Date          time.Time
	DateStr       string  `json:"date" parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Ticker        string  `json:"ticker" parquet:"name=ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi string  `json:"compositeFigi" parquet:"name=compositeFigi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Open          float32 `json:"open" parquet:"name=open, type=FLOAT"`
	High          float32 `json:"high" parquet:"name=high, type=FLOAT"`
	Low           float32 `json:"low" parquet:"name=low, type=FLOAT"`
	Close         float32 `json:"close" parquet:"name=close, type=FLOAT"`
	Volume        float32 `json:"volume" parquet:"name=volume, type=FLOAT"`
	Dividend      float32 `json:"divCash" parquet:"name=dividend, type=FLOAT"`
	Split         float32 `json:"splitFactor" parquet:"name=split, type=FLOAT"`
}

func New(token string, rateLimit int) *TiingoApi {
	t := &TiingoApi{
		token: token,
		rate:  ratelimit.New(rateLimit),
	}
	return t
}

func (t *TiingoApi) FetchEodQuotes(assets []*common.Asset, startDate time.Time) []*Eod {
	nyc, _ := time.LoadLocation("America/New_York")
	quotes := []*Eod{}
	client := resty.New()
	startDateStr := startDate.Format("2006-01-02")

	var bar *progressbar.ProgressBar
	if !viper.GetBool("display.hide_progress") {
		bar = progressbar.Default(int64(len(assets)))
	}
	chans := make([]chan Eod, 0, len(assets))
	for _, asset := range assets {
		// rate limiting
		t.rate.Take()

		// update progress
		if bar != nil {
			bar.Add(1)
		}

		// run download in parallel
		resultChan := make(chan Eod, 10)
		chans = append(chans, resultChan)

		go func(myAsset *common.Asset, myResultChan chan Eod) {
			defer close(myResultChan)
			// translate ticker to Tiingo ticker format; i.e. / turns to -
			ticker := strings.ReplaceAll(myAsset.Ticker, "/", "-")
			url := fmt.Sprintf("https://api.tiingo.com/tiingo/daily/%s/prices?startDate=%s&token=%s", ticker, startDateStr, t.token)
			resp, err := client.
				R().
				SetHeader("Accept", "application/json").
				Get(url)
			if err != nil {
				log.Error().Err(err).Str("Url", url).Msg("error when requesting eod quote")
				return
			}
			if resp.StatusCode() >= 400 {
				log.Error().Int("StatusCode", resp.StatusCode()).Str("Url", url).Bytes("Body", resp.Body()).Msg("error when requesting eod quote")
				return
			}
			data := resp.Body()
			var quote []Eod
			if err = json.Unmarshal(data, &quote); err != nil {
				log.Error().Err(err).Str("Ticker", myAsset.Ticker).Msg("could not unmarshal json")
			} else {
				for _, q := range quote {
					q.Ticker = myAsset.Ticker
					q.CompositeFigi = myAsset.CompositeFigi
					date, err := time.Parse(time.RFC3339, q.DateStr)
					if err == nil {
						q.Date = time.Date(date.Year(), date.Month(), date.Day(), 16, 0, 0, 0, nyc)
					}
					myResultChan <- q
				}
			}
		}(asset, resultChan)
	}

	for _, ch := range chans {
		// read individual eod values
		for val := range ch {
			copy := val
			quotes = append(quotes, &copy)
		}
	}

	return quotes
}

// SaveToParquet saves EOD quotes to a parquet file
func SaveToParquet(records []*Eod, fn string) error {
	var err error

	fh, err := local.NewLocalFileWriter(fn)
	if err != nil {
		log.Error().Err(err).Str("FileName", fn).Msg("cannot create local file")
		return err
	}
	defer fh.Close()

	pw, err := writer.NewParquetWriter(fh, new(Eod), 4)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Parquet write failed")
		return err
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8k
	pw.CompressionType = parquet.CompressionCodec_GZIP

	for _, r := range records {
		if err = pw.Write(r); err != nil {
			log.Error().
				Err(err).
				Str("EventDate", r.DateStr).
				Str("Ticker", r.Ticker).
				Str("CompositeFigi", r.CompositeFigi).
				Msg("Parquet write failed for record")
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Error().Err(err).Msg("Parquet write failed")
		return err
	}

	log.Info().Int("NumRecords", len(records)).Msg("Parquet write finished")
	return nil
}

// SaveToDatabase saves EOD quotes to the penny vault database
func SaveToDatabase(quotes []*Eod) error {
	log.Info().Msg("saving to database")
	conn, err := pgx.Connect(context.Background(), viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to database")
	}
	defer conn.Close(context.Background())

	for _, quote := range quotes {
		_, err := conn.Exec(context.Background(),
			`INSERT INTO eod (
			"ticker",
			"composite_figi",
			"event_date",
			"open",
			"high",
			"low",
			"close",
			"volume",
			"dividend",
			"split_factor",
			"source"
		) VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10,
			$11
		) ON CONFLICT ON CONSTRAINT eod_pkey
		DO UPDATE SET
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			dividend = EXCLUDED.dividend,
			split_factor = EXCLUDED.split_factor,
			source = EXCLUDED.source;`,
			quote.Ticker, quote.CompositeFigi, quote.Date,
			quote.Open, quote.High, quote.Low, quote.Close, quote.Volume,
			quote.Dividend, quote.Split, "api.tiingo.com")
		if err != nil {
			query := fmt.Sprintf(`INSERT INTO eod_v1 ("ticker", "composite_figi", "event_date", "open", "high", "low", "close", "volume", "dividend", "split_factor", "source") VALUES ('%s', '%s', '%s', %.5f, %.5f, %.5f, %.5f, %d, %.5f, %.5f, '%s') ON CONFLICT ON CONSTRAINT eod_v1_pkey DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume, dividend = EXCLUDED.dividend, split_factor = EXCLUDED.split_factor, source = EXCLUDED.source;`,
				quote.Ticker, quote.CompositeFigi, quote.Date,
				quote.Open, quote.High, quote.Low, quote.Close, int(quote.Volume),
				quote.Dividend, quote.Split, "api.tiingo.com")
			log.Error().Err(err).Str("Query", query).Msg("error saving EOD quote to database")
		}
	}

	return nil
}
