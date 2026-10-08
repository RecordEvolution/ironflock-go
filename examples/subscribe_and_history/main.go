// Streams the stored rows of a table and reads its history: the latest value
// per entity, a filtered range, and a down-sampled series.
package main

import (
	"context"
	"log"
	"time"

	ironflock "github.com/RecordEvolution/ironflock-go"
)

// Reading is a row of the "sensordata" table; columns are matched by the json
// tags. The data backend returns timestamps as epoch milliseconds.
type Reading struct {
	Tsp         int64   `json:"tsp"`
	DeviceKey   int     `json:"device_key"`
	Temperature float64 `json:"temperature"`
}

func main() {
	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		// Realtime: one event per stored row, bulk inserts included. The
		// subscription survives reconnects.
		_, err := ifl.SubscribeToTable(ctx, "sensordata", func(ev *ironflock.Event) {
			log.Printf("new row: %v", ev.Row())
		})
		if err != nil {
			return err
		}

		// Current value per entity.
		latest, err := ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
			Limit:     100,
			FilterAnd: []ironflock.Filter{ironflock.Latest()},
		})
		if err != nil {
			return err
		}
		readings, err := ironflock.DecodeRows[Reading](latest)
		if err != nil {
			return err
		}
		for _, r := range readings {
			log.Printf("device %d: %.1f°C at %s", r.DeviceKey, r.Temperature, time.UnixMilli(r.Tsp).UTC())
		}

		// The last day, warm readings only.
		warm, err := ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
			Limit:     500,
			TimeRange: ironflock.Since(time.Now().Add(-24 * time.Hour)),
			FilterAnd: []ironflock.Filter{ironflock.Where("temperature", ">", 25)},
			Columns:   []string{"temperature"},
		})
		if err != nil {
			return err
		}
		log.Printf("%d warm readings", len(warm))

		// Hourly averages for a chart.
		series, err := ifl.GetSeriesHistory(ctx, "sensordata", ironflock.SeriesQueryParams{
			Metrics:   []string{"temperature"},
			Method:    ironflock.MethodAvg,
			Limit:     24,
			TimeRange: ironflock.Since(time.Now().Add(-24 * time.Hour)),
		})
		if err != nil {
			return err
		}
		log.Printf("series: %v", series)

		<-ctx.Done() // keep streaming until SIGINT/SIGTERM
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
