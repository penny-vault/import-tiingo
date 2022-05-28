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
package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/penny-vault/import-tiingo/common"
	"github.com/penny-vault/import-tiingo/tiingo"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string
var maxAssets int

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "import-tiingo [tickers]",
	Short: "Download end-of-day quotes from tiingo",
	Long:  `Download end-of-day quotes from tiingo and save to penny-vault database`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().
			Dur("History", viper.GetDuration("tiingo.history")).
			Msg("loading tickers")

		assets := common.ReadAssetsFromDatabase(validatedAssetTypes)
		if maxAssets > 0 {
			assets = assets[:maxAssets]
		}

		t := tiingo.New(viper.GetString("tiingo.token"), viper.GetInt("tiingo.rate_limit"))
		startDate := time.Now().Add(viper.GetDuration("tiingo.history") * -1)
		quotes := t.FetchEodQuotes(assets, startDate)

		if viper.GetString("parquet_file") != "" {
			tiingo.SaveToParquet(quotes, viper.GetString("parquet_file"))
		}

		if viper.GetString("database.url") != "" {
			tiingo.SaveToDatabase(quotes)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	cobra.OnInitialize(initLog)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is import-tiingo.toml)")
	rootCmd.PersistentFlags().Bool("log.json", false, "print logs as json to stderr")
	viper.BindPFlag("log.json", rootCmd.PersistentFlags().Lookup("log.json"))

	rootCmd.PersistentFlags().StringP("tiingo-token", "t", "<not-set>", "tiingo API key token")
	viper.BindPFlag("tiingo.token", rootCmd.PersistentFlags().Lookup("tiingo-token"))

	rootCmd.PersistentFlags().StringP("database-url", "d", "host=localhost port=5432", "DSN for database connection")
	viper.BindPFlag("database.url", rootCmd.PersistentFlags().Lookup("database-url"))

	rootCmd.PersistentFlags().Duration("history", 24*7*time.Hour, "amount of history to download")
	viper.BindPFlag("tiingo.history", rootCmd.PersistentFlags().Lookup("history"))

	rootCmd.PersistentFlags().Int("tiingo-rate-limit", 5, "tiingo rate limit (items per second)")
	viper.BindPFlag("tiingo.rate_limit", rootCmd.PersistentFlags().Lookup("tiingo-rate-limit"))

	rootCmd.PersistentFlags().String("parquet-file", "", "save results to parquet")
	viper.BindPFlag("parquet_file", rootCmd.PersistentFlags().Lookup("parquet-file"))

	rootCmd.PersistentFlags().Bool("hide-progress", false, "hide progress bar")
	viper.BindPFlag("display.hide_progress", rootCmd.PersistentFlags().Lookup("hide-progress"))

	rootCmd.Flags().IntVar(&maxAssets, "max", -1, "maximum assets to download")
}

func initLog() {
	if !viper.GetBool("log.json") {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".import-tiingo" (without extension).
		viper.AddConfigPath("/etc") // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.config", home))
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
		viper.SetConfigName("import-tiingo")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debug().Str("ConfigFile", viper.ConfigFileUsed()).Msg("Loaded config file")
	} else {
		log.Error().Err(err).Msg("error reading config file")
	}
}
