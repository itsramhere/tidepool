package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"
)

type options struct {
	URL, Token          string
	Clients, Docs, Rate int
	Duration            time.Duration
}

func main() {
	o := options{}
	flag.StringVar(&o.URL, "url", "ws://127.0.0.1:7100/v1/sync", "sync endpoint")
	flag.StringVar(&o.Token, "token", os.Getenv("TIDEPOOL_TOKEN"), "JWT (or TIDEPOOL_TOKEN)")
	flag.IntVar(&o.Clients, "clients", 1000, "concurrent clients")
	flag.IntVar(&o.Docs, "docs", 100, "shared documents")
	flag.IntVar(&o.Rate, "rate", 5000, "aggregate offered operations per second")
	flag.DurationVar(&o.Duration, "duration", 60*time.Second, "measurement duration")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	report, err := run(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tpload:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if report.Errors > 0 {
		os.Exit(1)
	}
}
