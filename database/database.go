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
package database

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/penny-vault/import-dates/common"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func SyncTradingDays() (err error) {
	log.Info().Msg("saving to database")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
		return
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not begin transaction")
		return
	}

	rows, err := conn.Query(ctx, `SELECT event_date FROM eod WHERE ticker = $1 ORDER BY event_date ASC`, viper.GetString("history_ticker"))
	days := make([]time.Time, 0, 252*100)
	for rows.Next() {
		var tradingDay time.Time
		if err = rows.Scan(&tradingDay); err != nil {
			log.Error().Err(err).Msg("fetch existing trading days failed")
			tx.Rollback(ctx)
			return
		}
		days = append(days, tradingDay)
	}

	for _, tradingDay := range days {
		if _, err = tx.Exec(ctx, `INSERT INTO trading_days ("trading_day", "market") VALUES ($1, 'us') ON CONFLICT ON CONSTRAINT trading_days_pkey DO NOTHING`, tradingDay); err != nil {
			log.Error().Err(err).Time("TradingDay", tradingDay).Msg("insert trading day failed")
			tx.Rollback(ctx)
			return
		}
	}

	if err = tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("failed to commit trading days to database")
	}
	return
}

func SaveMarketHolidays(holidays []*common.MarketHoliday) (err error) {
	log.Info().Msg("saving to database")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("could not connect to database")
		return
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not begin transaction")
		return
	}

	for _, holiday := range holidays {
		log.Info().
			Str("Holiday", holiday.Name).
			Time("EventDate", holiday.Date).
			Bool("EarlyClose", holiday.EarlyClose).
			Msg("adding holiday")

		_, err = tx.Exec(ctx, `INSERT INTO market_holidays (
			"holiday",
			"event_date",
			"market",
			"early_close",
			"close_time"
		) VALUES (
			$1,
			$2,
			$3,
			$4,
			$5
		) ON CONFLICT ON CONSTRAINT market_holidays_pkey
		DO UPDATE SET
			holiday = EXCLUDED.holiday,
			early_close = EXCLUDED.early_close,
			close_time = EXCLUDED.close_time`, holiday.Name, holiday.Date, holiday.Market, holiday.EarlyClose, holiday.CloseTime)

		if err != nil {
			log.Error().Err(err).Msg("could not save market holiay to database")
			tx.Rollback(ctx)
			return err
		}
	}

	if err = tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("could not commit holidays to database")
	}

	return err
}
