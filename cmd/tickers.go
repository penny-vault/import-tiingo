// Copyright 2021
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"os"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/penny-vault/import-tiingo/common"
	"github.com/penny-vault/import-tiingo/tiingo"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(tickerCmd)
}

func printTable(quotes []*tiingo.Eod) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Date", "Ticker", "Open", "High", "Low", "Close", "Volume", "Dividend", "Split"})
	for _, quote := range quotes {
		t.AppendRow(table.Row{
			quote.Date, quote.Ticker, quote.Open, quote.High, quote.Low, quote.Close, quote.Volume, quote.Dividend, quote.Split,
		})
	}
	t.Render()
}

var tickerCmd = &cobra.Command{
	Use:   "ticker [ticker...]",
	Args:  cobra.MinimumNArgs(1),
	Short: "Download eod quotes for the given tickers",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().
			Dur("History", viper.GetDuration("tiingo.history")).
			Int("NumAssets", len(args)).
			Msg("loading tickers")

		var assets []*common.Asset
		for _, arg := range args {
			asset := &common.Asset{
				Ticker: arg,
			}
			assets = append(assets, asset)
		}

		t := tiingo.New(viper.GetString("tiingo.token"), viper.GetInt("tiingo.rate_limit"))
		startDate := time.Now().Add(viper.GetDuration("tiingo.history") * -1)
		quotes := t.FetchEodQuotes(assets, startDate)

		printTable(quotes)
	},
}
