package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/isard-vdi/CAS_Exporter/casexporter"
	"github.com/isard-vdi/CAS_Exporter/transport/http"
)

var addr string

func main() {
	addr := flag.String("addr", "0.0.0.0:2114", "Address to listen for HTTP metrics extraction (/metrics)")
	extractionInterval := flag.Duration("extraction-interval", 30*time.Second, "Interval between stats extraction")

	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	c := casexporter.NewCasExporter(*extractionInterval)

	go c.Start(ctx, &wg)
	wg.Add(1)

	http := http.ExporterServer{
		Addr:        *addr,
		CasExporter: c,
	}

	go http.Serve(ctx, &wg)
	wg.Add(1)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop
	fmt.Println("")
	slog.Info("stopping service")

	cancel()

	wg.Wait()
}
