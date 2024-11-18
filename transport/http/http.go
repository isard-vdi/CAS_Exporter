package http

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/isard-vdi/CAS_Exporter/casexporter"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ExporterServer struct {
	Addr        string
	CasExporter *casexporter.CasExporter
}

func (s *ExporterServer) Serve(ctx context.Context, wg *sync.WaitGroup) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(version.NewCollector("ocf"))
	reg.MustRegister(s.CasExporter)

	m := http.NewServeMux()
	m.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		promhttp.HandlerFor(prometheus.Gatherers{reg}, promhttp.HandlerOpts{
			ErrorHandling:       promhttp.ContinueOnError,
			MaxRequestsInFlight: 40,
		}).ServeHTTP(w, r)

		slog.Info("stats served",
			slog.Duration("duration", time.Since(start)),
		)
	})

	srv := http.Server{
		Addr:    s.Addr,
		Handler: m,
	}

	go func() {
		slog.Info("listening http for extraction",
			slog.String("addr", s.Addr),
		)
		if err := srv.ListenAndServe(); err != nil {
			slog.Error("serve http",
				slog.String("err", err.Error()),
				slog.String("addr", s.Addr),
			)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv.Shutdown(timeout)
	wg.Done()
}
