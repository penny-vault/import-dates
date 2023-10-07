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
package polygon

import (
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/penny-vault/import-dates/common"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type PolygonHoliday struct {
	Exchange string
	Name     string
	Date     string
	Status   string
	Open     string
	Close    string
}

func MarketHolidays() (holidays []*common.MarketHoliday, err error) {
	var nyc *time.Location
	nyc, err = time.LoadLocation("America/New_York")
	if err != nil {
		log.Error().Err(err).Msg("could not load nyc timezone")
		return
	}

	var resp *resty.Response
	client := resty.New()

	polygonHolidays := make([]PolygonHoliday, 0, 50)
	url := fmt.Sprintf("https://api.polygon.io/v1/marketstatus/upcoming?apiKey=%s", viper.GetString("polygon.token"))
	if resp, err = client.R().SetResult(&polygonHolidays).Get(url); err != nil {
		log.Error().Err(err).Msg("unable to retrieve polygon market holidays")
		return
	} else {
		if resp.StatusCode() >= 300 {
			err = fmt.Errorf("invalid status code returned from polygon/marketstatus")
			log.Error().Err(err).Msg("invalid status code")
			return
		}
	}

	for _, holiday := range polygonHolidays {
		if holiday.Exchange == "NASDAQ" {
			var date time.Time
			date, err = time.Parse("2006-01-02", holiday.Date)
			if err != nil {
				log.Error().Err(err).Msg("could not parse date")
				return
			}
			date = time.Date(date.Year(), date.Month(), date.Day(), 16, 0, 0, 0, nyc)

			var close time.Time
			close, err = time.Parse("2006-01-02T15:04:05Z07:00", holiday.Close)
			if err != nil {
				close = date
			} else {
				close = close.In(nyc)
			}

			holiday := &common.MarketHoliday{
				Name:       holiday.Name,
				Date:       date,
				Market:     "us",
				EarlyClose: holiday.Status == "early-close",
				CloseTime:  close,
			}
			holidays = append(holidays, holiday)
		}
	}

	return holidays, nil
}
