package casexporter

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/isard-vdi/CAS_Exporter/casadm"

	"github.com/prometheus/client_golang/prometheus"
)

func NewCasExporter(extractionInterval time.Duration) *CasExporter {
	return &CasExporter{
		extractionInterval: extractionInterval,

		ocfStatCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ocf_count",
				Help: "OCF count value",
			},
			[]string{"device", "id", "category", "subcategory"},
		),
		ocfStatPercentage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ocf_percentage",
				Help: "OCF percentage value",
			},
			[]string{"device", "id", "category", "subcategory"},
		),
		ocfStatDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ocf_duration_seconds",
				Help: "OCF stats extraction duration",
			},
			[]string{},
		),
		ocfStatSuccess: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ocf_success",
				Help: "Whether OCF stats extraction has succeeded",
			},
			[]string{},
		),
	}
}

type CasExporter struct {
	extractionInterval time.Duration

	ocfStatCount      *prometheus.GaugeVec
	ocfStatPercentage *prometheus.GaugeVec
	ocfStatDuration   *prometheus.GaugeVec
	ocfStatSuccess    *prometheus.GaugeVec
}

func (e *CasExporter) Describe(ch chan<- *prometheus.Desc) {
	e.ocfStatCount.Describe(ch)
	e.ocfStatPercentage.Describe(ch)
	e.ocfStatDuration.Describe(ch)
	e.ocfStatSuccess.Describe(ch)
}

func (e *CasExporter) Collect(ch chan<- prometheus.Metric) {
	e.ocfStatCount.Collect(ch)
	e.ocfStatPercentage.Collect(ch)
	e.ocfStatDuration.Collect(ch)
	e.ocfStatSuccess.Collect(ch)
}

// TODO: Do scraping and collection in two different threads?
func (e *CasExporter) Start(ctx context.Context, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return

		default:
			start := time.Now()

			success := 1

			caches, err := casadm.ListCaches(ctx)
			if err != nil {
				success = 0
				slog.Error("list caches",
					slog.String("err", err.Error()),
				)

			} else {
				for _, c := range caches {
					if c.Device == "-" {
						continue
					}

					stats, err := casadm.GetCacheStats(ctx, c.ID)
					if err != nil {
						success = 0
						slog.Error("get cache stats",
							slog.Int("cache_id", int(c.ID)),
							slog.String("err", err.Error()),
						)

						continue
					}

					//
					// Count
					//

					// Usage
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "usage",
						"subcategory": "occupancy",
					}).Set(float64(stats.Occupancy4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "usage",
						"subcategory": "free",
					}).Set(float64(stats.Free4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "usage",
						"subcategory": "clean",
					}).Set(float64(stats.Clean4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "usage",
						"subcategory": "dirty",
					}).Set(float64(stats.Dirty4K))

					// Requests
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_hits",
					}).Set(float64(stats.ReadHitsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_partial_misses",
					}).Set(float64(stats.ReadPartialMissesRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_full_misses",
					}).Set(float64(stats.ReadFullMissesRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_total",
					}).Set(float64(stats.ReadTotalRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_hits",
					}).Set(float64(stats.WriteHitsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_partial_misses",
					}).Set(float64(stats.WritePartialMissesRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_full_misses",
					}).Set(float64(stats.WriteFullMissesRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_total",
					}).Set(float64(stats.WriteTotalRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_pt",
					}).Set(stats.ReadTotalPercent)
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_pt",
					}).Set(stats.WriteTotalPercent)
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "serviced",
					}).Set(float64(stats.ServicedRequestsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "total",
					}).Set(float64(stats.TotalRequestsRequests))

					// Blocks
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "core_volume_rd",
					}).Set(float64(stats.ReadsFromCores4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "core_volume_wr",
					}).Set(float64(stats.WritesFromCores4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "core_volume_total",
					}).Set(float64(stats.TotalToFromCores4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "cache_volume_rd",
					}).Set(float64(stats.ReadsFromCache4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "cache_volume_wr",
					}).Set(float64(stats.WritesToCachce4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "cache_volume_total",
					}).Set(float64(stats.TotalToFromCache4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "volume_rd",
					}).Set(float64(stats.ReadsFromExportedObjects4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "volume_wr",
					}).Set(float64(stats.WritesToExportedObjects4K))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "volume_total",
					}).Set(float64(stats.TotalToFromExportedObjects4K))

					// Errors
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "cache_volume_rd",
					}).Set(float64(stats.CacheReadErrorsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "cache_volume_wr",
					}).Set(float64(stats.CacheWriteErrorsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "cache_volume_total",
					}).Set(float64(stats.CacheTotalErrorsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "core_volume_rd",
					}).Set(float64(stats.CoreReadErrorsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "core_volume_wr",
					}).Set(float64(stats.CoreWriteErrorsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "core_volume_total",
					}).Set(float64(stats.CoreTotalErrorsRequests))
					e.ocfStatCount.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "total",
					}).Set(float64(stats.TotalErrorsRequests))

					//
					//  Percent
					//

					// Usage
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "usage",
						"subcategory": "occupancy",
					}).Set(stats.OccupancyPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "usage",
						"subcategory": "free",
					}).Set(stats.FreePercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "usage",
						"subcategory": "clean",
					}).Set(stats.CleanPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "usage",
						"subcategory": "dirty",
					}).Set(stats.DirtyPercent)

					// Requests
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_hits",
					}).Set(stats.ReadHitsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_partial_misses",
					}).Set(stats.ReadPartialMissesPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_full_misses",
					}).Set(stats.ReadFullMissesPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_total",
					}).Set(stats.ReadTotalPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_hits",
					}).Set(stats.WriteHitsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_partial_misses",
					}).Set(stats.WritePartialMissesPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_full_misses",
					}).Set(stats.WriteFullMissesPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_total",
					}).Set(stats.WriteTotalPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "rd_pt",
					}).Set(stats.ReadTotalPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "wr_pt",
					}).Set(stats.WriteTotalPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "serviced",
					}).Set(stats.ServicedRequestsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "requests",
						"subcategory": "total",
					}).Set(stats.TotalRequestsPercent)

					// Blocks
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "core_volume_rd",
					}).Set(stats.ReadsFromCoresPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "core_volume_wr",
					}).Set(stats.WritesFromCoresPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "core_volume_total",
					}).Set(stats.TotalToFromCoresPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "cache_volume_rd",
					}).Set(stats.ReadsFromCachePercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "cache_volume_wr",
					}).Set(stats.WritesToCachcePercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "cache_volume_total",
					}).Set(stats.TotalToFromCachePercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "volume_rd",
					}).Set(stats.ReadsFromExportedObjectsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "volume_wr",
					}).Set(stats.WritesToExportedObjectsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "blocks",
						"subcategory": "volume_total",
					}).Set(stats.TotalToFromExportedObjectsPercent)

					// Errors
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "cache_volume_rd",
					}).Set(stats.CacheReadErrorsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "cache_volume_wr",
					}).Set(stats.CacheWriteErrorsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "cache_volume_total",
					}).Set(stats.CacheTotalErrorsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "core_volume_rd",
					}).Set(stats.CoreReadErrorsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "core_volume_wr",
					}).Set(stats.CoreWriteErrorsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "core_volume_total",
					}).Set(stats.CoreTotalErrorsPercent)
					e.ocfStatPercentage.With(prometheus.Labels{
						"device":      c.Device,
						"id":          strconv.Itoa(int(c.ID)),
						"category":    "errors",
						"subcategory": "total",
					}).Set(stats.TotalErrorsPercent)

				}
			}

			duration := time.Since(start)

			e.ocfStatDuration.With(prometheus.Labels{}).Set(duration.Seconds())
			e.ocfStatSuccess.With(prometheus.Labels{}).Set(float64(success))

			slog.Info("extracted opencas stats",
				slog.Duration("duration", duration),
				slog.Bool("success", success == 1),
			)

			time.Sleep(e.extractionInterval)
		}
	}
}
