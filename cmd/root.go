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

	"github.com/penny-vault/import-dates/database"
	"github.com/penny-vault/import-dates/polygon"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "import-dates",
	Short: "Add info about trading days and market holidays to database",
	Run: func(cmd *cobra.Command, args []string) {
		if holidays, err := polygon.MarketHolidays(); err == nil {
			database.SaveMarketHolidays(holidays)
		}
		database.SyncTradingDays()
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

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.import-dates.yaml)")
	rootCmd.PersistentFlags().Bool("log.json", false, "print logs as json to stderr")
	viper.BindPFlag("log.json", rootCmd.PersistentFlags().Lookup("log.json"))
	rootCmd.PersistentFlags().StringP("database-url", "d", "host=localhost port=5432", "DSN for database connection")
	viper.BindPFlag("database.url", rootCmd.PersistentFlags().Lookup("database-url"))
	rootCmd.PersistentFlags().String("polygon-token", "<not-set>", "polygon API key token")
	viper.BindPFlag("polygon.token", rootCmd.PersistentFlags().Lookup("polygon-token"))
	rootCmd.PersistentFlags().String("history-ticker", "SPY", "ticker to use for tading history")
	viper.BindPFlag("history_ticker", rootCmd.PersistentFlags().Lookup("history-ticker"))
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

		viper.AddConfigPath("/etc") // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.config", home))
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
		viper.SetConfigName("import-dates")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debug().Str("ConfigFile", viper.ConfigFileUsed()).Msg("Loaded config file")
	} else {
		log.Error().Err(err).Msg("error reading config file")
	}
}

func initLog() {
	if !viper.GetBool("log.json") {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}
