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
	"archive/zip"
	"bytes"
	"io/ioutil"

	"github.com/go-resty/resty/v2"
	"github.com/gocarina/gocsv"
	"github.com/rs/zerolog/log"
)

type Eod struct {
	Date           string  `json:"date"`
	Ticker         string  `json:"ticker"`
	CompositeFigi  string  `json:"compositeFigi"`
	Open           float32 `json:"open"`
	High           float32 `json:"high"`
	Low            float32 `json:"low"`
	Close          float32 `json:"close"`
	Volume         int64   `json:"volume"`
	AdjustedOpen   float32 `json:"adjOpen"`
	AdjustedHigh   float32 `json:"adjHigh"`
	AdjustedLow    float32 `json:"adjLow"`
	AdjustedClose  float32 `json:"adjClose"`
	AdjustedVolume int64   `json:"adjVolume"`
	Dividend       float32 `json:"divCash"`
	Split          float32 `json:"splitFactor"`
}

type Asset struct {
	Ticker        string `json:"ticker" csv:"ticker"`
	Exchange      string `json:"exchange" csv:"exchange"`
	AssetType     string `json:"assetType" csv:"assetType"`
	PriceCurrency string `json:"priceCurrency" csv:"priceCurrency"`
	StartDate     string `json:"startDate" csv:"startDate"`
	EndDate       string `json:"endDate" csv:"endDate"`
}

func readZipFile(zf *zip.File) ([]byte, error) {
	f, err := zf.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ioutil.ReadAll(f)
}

// DownloadTickers fetches a list of supported tickers from Tiingo
func FetchTickers() []*Asset {
	tickerUrl := "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
	client := resty.New()
	assets := []*Asset{}

	resp, err := client.
		R().
		Get(tickerUrl)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to download tickers")
		return assets
	}

	// unzip downloaded data
	body := resp.Body()
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("could not read response body when downloading tickers")
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to read tickers zip file")
	}

	// Read all the files from zip archive
	var tickerCsvBytes []byte
	if len(zipReader.File) == 0 {
		log.Error().Msg("no files contained in received zip file")
		return assets
	}

	zipFile := zipReader.File[0]
	tickerCsvBytes, err = readZipFile(zipFile)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to read ticker csv from zip")
		return assets
	}

	if err := gocsv.UnmarshalBytes(tickerCsvBytes, &assets); err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("failed to unmarshal csv")
		return assets
	}

	return assets
}

// FilterExchange removes assets that are not traded on one of the listed exchanges
func FilterExchange(assets []*Asset, exchanges []string) []*Asset {
	res := make([]*Asset, 0, len(assets))
	for _, asset := range assets {
		exchangeFound := false
		for _, exchange := range exchanges {
			if asset.Exchange == exchange {
				exchangeFound = true
			}
		}
		if exchangeFound {
			res = append(res, asset)
		}
	}
	return res
}
