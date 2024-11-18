package casadm

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"

	"github.com/gocarina/gocsv"
)

const casaCmd = "casadm"

type Cache struct {
	Type        string `csv:"type"`
	ID          uint16 `csv:"id"`
	Disk        string `csv:"disk"`
	Status      string `csv:"status"`
	WritePolicy string `csv:"write policy"`
	Device      string `csv:"device"`
}

func ListCaches(ctx context.Context) ([]*Cache, error) {
	b, err := exec.CommandContext(ctx, casaCmd, "--list-caches", "--output-format", "csv").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("list caches: %w: '%s'", err, b)
	}

	caches := []*Cache{}

	if err := gocsv.UnmarshalBytes(b, &caches); err != nil {
		return nil, fmt.Errorf("unmarshal list caches csv: %w", err)
	}

	return caches, nil
}

type CacheStats struct {
	ID                                uint16  `csv:"Cache Id"`
	Size4K                            float64 `csv:"Cache Size [4KiB Blocks]"`
	SizeGB                            float64 `csv:"Cache Size [GiB]"`
	Device                            string  `csv:"Cache Device"`
	ExportedObject                    string  `csv:"Exported Object"`
	CoreDevices                       int     `csv:"Core Devices"`
	InactiveCoreDevices               int     `csv:"Inactive Core Devices"`
	WritePolicy                       string  `csv:"Write Policy"`
	CleaningPolicy                    string  `csv:"Cleaning Policy"`
	PromotionPolicy                   string  `csv:"Promotion Policy"`
	CacheLineSizeKB                   float64 `csv:"Cache line size [KiB]"`
	MetadataMemoryFootprintMB         float64 `csv:"Metadata Memory Footprint [MiB]"`
	DirtyForS                         int     `csv:"Dirty for [s]"`
	DirtyFor                          string  `csv:"Dirty for"`
	Status                            string  `csv:"Status"`
	Occupancy4K                       int     `csv:"Occupancy [4KiB Blocks]"`
	OccupancyPercent                  float64 `csv:"Occupancy [%]"`
	Free4K                            int     `csv:"Free [4KiB Blocks]"`
	FreePercent                       float64 `csv:"Free [%]"`
	Clean4K                           int     `csv:"Clean [4KiB Blocks]"`
	CleanPercent                      float64 `csv:"Clean [%]"`
	Dirty4K                           int     `csv:"Dirty [4KiB Blocks]"`
	DirtyPercent                      float64 `csv:"Dirty [%]"`
	ReadHitsRequests                  int     `csv:"Read hits [Requests]"`
	ReadHitsPercent                   float64 `csv:"Read hits [%]"`
	ReadPartialMissesRequests         int     `csv:"Read partial misses [Requests]"`
	ReadPartialMissesPercent          float64 `csv:"Read partial misses [%]"`
	ReadFullMissesRequests            int     `csv:"Read full misses [Requests]"`
	ReadFullMissesPercent             float64 `csv:"Read full misses [%]"`
	ReadTotalRequests                 int     `csv:"Read total [Requests]"`
	ReadTotalPercent                  float64 `csv:"Read total [%]"`
	WriteHitsRequests                 int     `csv:"Write hits [Requests]"`
	WriteHitsPercent                  float64 `csv:"Write hits [%]"`
	WritePartialMissesRequests        int     `csv:"Write partial misses [Requests]"`
	WritePartialMissesPercent         float64 `csv:"Write partial misses [%]"`
	WriteFullMissesRequests           int     `csv:"Write full misses [Requests]"`
	WriteFullMissesPercent            float64 `csv:"Write full misses [%]"`
	WriteTotalRequests                int     `csv:"Write total [Requests]"`
	WriteTotalPercent                 float64 `csv:"Write total [%]"`
	PassThroughReadsRequests          int     `csv:"Pass-Through reads [Requests]"`
	PassThroughReadsPercent           float64 `csv:"Pass-Through reads [%]"`
	PassThroughWritesRequests         int     `csv:"Pass-Through writes [Requests]"`
	PassThroughWritesPercent          float64 `csv:"Pass-Through writes [%]"`
	ServicedRequestsRequests          int     `csv:"Serviced requests [Requests]"`
	ServicedRequestsPercent           float64 `csv:"Serviced requests [%]"`
	TotalRequestsRequests             int     `csv:"Total requests [Requests]"`
	TotalRequestsPercent              float64 `csv:"Total requests [%]"`
	ReadsFromCores4K                  int     `csv:"Reads from core(s) [4KiB Blocks]"`
	ReadsFromCoresPercent             float64 `csv:"Reads from core(s) [%]"`
	WritesFromCores4K                 int     `csv:"Writes to core(s) [4KiB Blocks]"`
	WritesFromCoresPercent            float64 `csv:"Writes to core(s) [%]"`
	TotalToFromCores4K                int     `csv:"Total to/from core(s) [4KiB Blocks]"`
	TotalToFromCoresPercent           float64 `csv:"Total to/from core(s) [%]"`
	ReadsFromCache4K                  int     `csv:"Reads from cache [4KiB Blocks]"`
	ReadsFromCachePercent             float64 `csv:"Reads from cache [%]"`
	WritesToCachce4K                  int     `csv:"Writes to cache [4KiB Blocks]"`
	WritesToCachcePercent             float64 `csv:"Writes to cache [%]"`
	TotalToFromCache4K                int     `csv:"Total to/from cache [4KiB Blocks]"`
	TotalToFromCachePercent           float64 `csv:"Total to/from cache [%]"`
	ReadsFromExportedObjects4K        int     `csv:"Reads from exported object(s) [4KiB Blocks]"`
	ReadsFromExportedObjectsPercent   float64 `csv:"Reads from exported object(s) [%]"`
	WritesToExportedObjects4K         int     `csv:"Writes to exported object(s) [4KiB Blocks]"`
	WritesToExportedObjectsPercent    float64 `csv:"Writes to exported object(s) [%]"`
	TotalToFromExportedObjects4K      int     `csv:"Total to/from exported object(s) [4KiB Blocks]"`
	TotalToFromExportedObjectsPercent float64 `csv:"Total to/from exported object(s) [%]"`
	CacheReadErrorsRequests           int     `csv:"Cache read errors [Requests]"`
	CacheReadErrorsPercent            float64 `csv:"Cache read errors [%]"`
	CacheWriteErrorsRequests          int     `csv:"Cache write errors [Requests]"`
	CacheWriteErrorsPercent           float64 `csv:"Cache write errors [%]"`
	CacheTotalErrorsRequests          int     `csv:"Cache total errors [Requests]"`
	CacheTotalErrorsPercent           float64 `csv:"Cache total errors [%]"`
	CoreReadErrorsRequests            int     `csv:"Core read errors [Requests]"`
	CoreReadErrorsPercent             float64 `csv:"Core read errors [%]"`
	CoreWriteErrorsRequests           int     `csv:"Core write errors [Requests]"`
	CoreWriteErrorsPercent            float64 `csv:"Core write errors [%]"`
	CoreTotalErrorsRequests           int     `csv:"Core total errors [Requests]"`
	CoreTotalErrorsPercent            float64 `csv:"Core total errors [%]"`
	TotalErrorsRequests               int     `csv:"Total errors [Requests]"`
	TotalErrorsPercent                float64 `csv:"Total errors [%]"`
}

func GetCacheStats(ctx context.Context, cacheID uint16) (*CacheStats, error) {
	b, err := exec.CommandContext(ctx, casaCmd, "--stats", "--cache-id", strconv.Itoa(int(cacheID)), "--output-format", "csv").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("list caches: %w: '%s'", err, b)
	}

	stats := []*CacheStats{}

	if err := gocsv.UnmarshalBytes(b, &stats); err != nil {
		return nil, fmt.Errorf("unmarshal cache stats csv: %w", err)
	}

	if len(stats) == 0 {
		return nil, errors.New("missing cache stats")
	}

	return stats[0], nil
}
