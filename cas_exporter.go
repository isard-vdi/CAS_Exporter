//##############################################################################
//# cas_exporter.go
//#
//#
//# Description:  This is a plugin for Prometheus to parse Open CAS Linux
//#               OCF data in order to visualize metrics in Grafana
//#
//# Usage:     cas_exporter [-port=PORT_NUMBER] | [-cache=CACHE_INSTANCE_NUM] |
//#                       [-log] | [-logfile=FULL_PATH_TO_LOG]  |
//#                       [-sleep=SECS_TO_SLEEP_BETWEEN_ITERATIONS]
//#
//#  Example:  cas_exporter -port=2114 -cache=1 -log -logfile="/tmp/cas_exporter.out" --sleep=1
//##############################################################################

package main

import (
    "fmt"
    "flag"
    "time"
    "os/exec"
    "log"
    "os"
    "strconv"
    "strings"
    "net/http"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

// Global variables
var (
  portNumber int
  sleepTime int
  isLogEnabled bool
  logPath string
  cache string
)

type OCF_data struct {
  Count  float64
  Percentage string
  Units string
}

type OCF_usage struct {
  Occupancy OCF_data
  Free OCF_data
  Clean OCF_data
  Dirty OCF_data
}

type OCF_requests struct {
  Rd_hits OCF_data
  Rd_partial_misses OCF_data
  Rd_full_misses OCF_data
  Rd_total OCF_data
  Wr_hits OCF_data
  Wr_partial_misses OCF_data
  Wr_full_misses OCF_data
  Wr_total OCF_data
  Rd_pt OCF_data
  Wr_pt OCF_data
  Serviced OCF_data
  Total OCF_data
}

type OCF_blocks struct {
  Core_volume_rd OCF_data
  Core_volume_wr OCF_data
  Core_volume_total OCF_data
  Cache_volume_rd OCF_data
  Cache_volume_wr OCF_data
  Cache_volume_total OCF_data
  Volume_rd OCF_data
  Volume_wr OCF_data
  Volume_total OCF_data
}

type OCF_errors struct {
  Core_volume_rd OCF_data
  Core_volume_wr OCF_data
  Core_volume_total OCF_data
  Cache_volume_rd OCF_data
  Cache_volume_wr OCF_data
  Cache_volume_total OCF_data
  Total OCF_data
}

type OCFStat struct {
  Usage OCF_usage
  Requests OCF_requests
  Blocks OCF_blocks
  Errors OCF_errors
}


// Definitions of header keywords.
// These are the keywords the function intializeHeaders will initialize to use internally for mapping
var (
  occupancy_blk         = "occupancy_blk"
  occupancy_pct         = "occupancy_pct"
  free_blk              = "free_blk"
  free_pct              = "free_pct"
  clean_blk             = "clean_blk"
  clean_pct             = "clean_pct"
  diry_blk              = "diry_blk"
  dirty_pct             = "dirty_pct"
  rd_hit_blk            = "rd_hit_blk"
  rd_hit_pct            = "rd_hit_pct"
  rd_part_misses_blk    = "rd_part_misses_blk"
  rd_part_misses_pct    = "rd_part_misses_pct"
  rd_full_misses_blk    = "rd_full_misses_blk"
  rd_full_misses_pct    = "rd_full_misses_pct"
  rd_total_blk          = "rd_total_blk"
  rd_total_pct          = "rd_total_pct"
  wt_hit_blk            = "wt_hit_blk"
  wt_hit_pct            = "wt_hit_pct"
  wt_part_misses_blk    = "wt_part_misses_blk"
  wt_part_misses_pct    = "wt_part_misses_pct"
  wt_full_misses_blk    = "wt_full_misses_blk"
  wt_full_misses_pct    = "wt_full_misses_pct"
  wt_total_blk          = "wt_total_blk"
  wt_total_pct          = "wt_total_pct"
  passthru_rd_blk       = "passthru_rd_blk"
  passthru_rd_pct       = "passthru_rd_pct"
  passthru_wt_blk       = "passthru_wt_blk"
  passthru_wt_pct       = "passthru_wt_pct"
  serviced_blk          = "serviced_blk"
  serviced_pct          = "serviced_pct"
  total_request_blk     = "total_request_blk"
  total_request_pct     = "total_request_pct"
  rd_core_blk           = "rd_core_blk"
  rd_core_pct           = "rd_core_pct"
  wt_core_blk           = "wt_core_blk"
  wt_core_pct           = "wt_core_pct"
  total_core_blk        = "total_core_blk"
  total_core_pct        = "total_core_pct"
  rd_cache_blk          = "rd_cache_blk"
  rd_cache_pct          = "rd_cache_pct"
  wt_cache_blk          = "wt_cache_blk"
  wt_cache_pct          = "wt_cache_pct"
  total_cache_blk       = "total_cache_blk"
  total_cache_pct       = "total_cache_pct"
  rd_cas_blk            = "rd_cas_blk"
  rd_cas_pct            = "rd_cas_pct"
  wt_cas_blk            = "wt_cas_blk"
  wt_cas_pct            = "wt_cas_pct"
  total_cas_blk         = "total_cas_blk"
  total_cas_pct         = "total_cas_pct"
  cache_rd_error_blk    = "cache_rd_error_blk"
  cache_rd_error_pct    = "cache_rd_error_pct"
  cache_wt_error_blk    = "cache_wt_error_blk"
  cache_wt_error_pct    = "cache_wt_error_pct"
  cache_total_error_blk = "cache_total_error_blk"
  cache_total_error_pct = "cache_total_error_pct"
  core_rd_error_blk     = "core_rd_error_blk"
  core_rd_error_pct     = "core_rd_error_pct"
  core_wt_error_blk     = "core_wt_error_blk"
  core_wt_error_pct     = "core_wt_error_pct"
  core_total_error_blk  = "core_total_error_blk"
  core_total_error_pct  = "core_total_error_pct"
  total_error_blk        = "total_error_blk"
  total_error_pct        = "total_error_pct"
)

// Variable to map the headers to a position in csv output
// the header keywords will be searched to find a position in func findHeaders
var headers = make(map[string]string)

// Will map header keywords to position in csv output
var headerMap = make(map[string]int)

// Definitions of metrics
var (
  OCFStat_count = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ocf_count",
			Help: "OCF count value",
		},
		[]string{"category", "subcategory"},
  )
  OCFStat_percentage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ocf_percentage",
			Help: "OCF percentage value",
		},
		[]string{"category", "subcategory"},
  )
)

//##############################################################################
//# Function: initializeHeaders
//#
//# Input:   None
//# Output:  headers
//#
//# Description:  This function will initialize the headers variable.
//#    The key to the map is the internal header name. The value to the map is
//#    the external header name (what appears in the csv file)
//##############################################################################
func initializeHeaders(){
  // The usage of these headers is:
  // headers[ internal_variable ] = string_to_search_in_csv_header
  // The string to search for is how mapHeaders function will find a position
  headers[occupancy_blk        ] = "Occupancy [4KiB blocks]"
  headers[occupancy_pct        ] = "Occupancy [%]"
  headers[free_blk             ] = "Free [4KiB blocks]"
  headers[free_pct             ] = "Free [%]"
  headers[clean_blk            ] = "Clean [4KiB blocks]"
  headers[clean_pct            ] = "Clean [%]"
  headers[diry_blk             ] = "Dirty [4KiB blocks]"
  headers[dirty_pct            ] = "Dirty [%]"
  headers[rd_hit_blk           ] = "Read hits [Requests]"
  headers[rd_hit_pct           ] = "Read hits [%]"
  headers[rd_part_misses_blk   ] = "Read partial misses [Requests]"
  headers[rd_part_misses_pct   ] = "Read partial misses [%]"
  headers[rd_full_misses_blk   ] = "Read full misses [Requests]"
  headers[rd_full_misses_pct   ] = "Read full misses [%]"
  headers[rd_total_blk         ] = "Read total [Requests]"
  headers[rd_total_pct         ] = "Read total [%]"
  headers[wt_hit_blk           ] = "Write hits [Requests]"
  headers[wt_hit_pct           ] = "Write hits [%]"
  headers[wt_part_misses_blk   ] = "Write partial misses [Requests]"
  headers[wt_part_misses_pct   ] = "Write partial misses [%]"
  headers[wt_full_misses_blk   ] = "Write full misses [Requests]"
  headers[wt_full_misses_pct   ] = "Write full misses [%]"
  headers[wt_total_blk         ] = "Write total [Requests]"
  headers[wt_total_pct         ] = "Write total [%]"
  headers[passthru_rd_blk      ] = "Pass-Through reads [Requests]"
  headers[passthru_rd_pct      ] = "Pass-Through reads [%]"
  headers[passthru_wt_blk      ] = "Pass-Through writes [Requests]"
  headers[passthru_wt_pct      ] = "Pass-Through writes [%]"
  headers[serviced_blk         ] = "Serviced requests [Requests]"
  headers[serviced_pct         ] = "Serviced requests [%]"
  headers[total_request_blk    ] = "Total requests [Requests]"
  headers[total_request_pct    ] = "Total requests [%]"
  headers[rd_core_blk          ] = "Reads from core(s) [4KiB blocks]"
  headers[rd_core_pct          ] = "Reads from core(s) [%]"
  headers[wt_core_blk          ] = "Writes to core(s) [4KiB blocks]"
  headers[wt_core_pct          ] = "Writes to core(s) [%]"
  headers[total_core_blk       ] = "Total to/from core(s) [4KiB blocks]"
  headers[total_core_pct       ] = "Total to/from core(s) [%]"
  headers[rd_cache_blk         ] = "Reads from cache [4KiB blocks]"
  headers[rd_cache_pct         ] = "Reads from cache [%]"
  headers[wt_cache_blk         ] = "Writes to cache [4KiB blocks]"
  headers[wt_cache_pct         ] = "Writes to cache [%]"
  headers[total_cache_blk      ] = "Total to/from cache [4KiB blocks]"
  headers[total_cache_pct      ] = "Total to/from cache [%]"
  headers[rd_cas_blk           ] = "Reads from exported object(s) [4KiB blocks]"
  headers[rd_cas_pct           ] = "Reads from exported object(s) [%]"
  headers[wt_cas_blk           ] = "Writes to exported object(s) [4KiB blocks]"
  headers[wt_cas_pct           ] = "Writes to exported object(s) [%]"
  headers[total_cas_blk        ] = "Total to/from exported object(s) [4KiB blocks]"
  headers[total_cas_pct        ] = "Total to/from exported object(s) [%]"
  headers[cache_rd_error_blk   ] = "Cache read errors [Requests]"
  headers[cache_rd_error_pct   ] = "Cache read errors [%]"
  headers[cache_wt_error_blk   ] = "Cache write errors [Requests]"
  headers[cache_wt_error_pct   ] = "Cache write errors [%]"
  headers[cache_total_error_blk] = "Cache total errors [Requests]"
  headers[cache_total_error_pct] = "Cache total errors [%]"
  headers[core_rd_error_blk    ] = "Core read errors [Requests]"
  headers[core_rd_error_pct    ] = "Core read errors [%]"
  headers[core_wt_error_blk    ] = "Core write errors [Requests]"
  headers[core_wt_error_pct    ] = "Core write errors [%]"
  headers[core_total_error_blk ] = "Core total errors [Requests]"
  headers[core_total_error_pct ] = "Core total errors [%]"
  headers[total_error_blk      ] = "Total errors [Requests]"
  headers[total_error_pct      ] = "Total errors [%]"
}

//##############################################################################
//# Function: mapHeaders
//#
//# Input:   header
//#          the string representing the header to parse
//# Output:  return_code
//#          returns 0 if successfully mapped all values or 1 if it did not.
//#
//# Description:  This function will map the headers from a csv file.
//#    The key to the map is the exact header string to get values for.
//#    the value is the position where that header string appearts in csv file.
//##############################################################################
func mapHeaders(headerline string) int{
  var all_keys []string
  var found = false

  for h := range headers {
    all_keys = append(all_keys, h)
  }

  //fmt.Println("DEBUG: headerline [" + headerline + "]")

  csv_headers := strings.Split(headerline, ",")

  for _, key := range all_keys {
    header_keyword := headers[key]
    // fmt.Println("DEBUG: header_keyword [" + header_keyword + "]")
    for i:= 0; i<len(csv_headers); i++ {
      csv_header := csv_headers[i]
    //  fmt.Println("DEBUG: csv_header [" + csv_header + "]")
      if strings.Contains(csv_header, header_keyword) {
         headerMap[header_keyword] = i
         //fmt.Println("DEBUG: header_keyword [" + header_keyword +"] was found in position [" + strconv.Itoa(i) + "]")
         found = true
         break
      }
    }
    if (found == false){
      fmt.Println("WARNING: did not find the header [" + header_keyword + "] in the csv output")
      return 1
    }
  }

  return 0
}


//##############################################################################
//# Function: check
//#
//# Input:   error
//# Output:  None
//#
//# Description:  This function will issue a system panic if error is not nil
//##############################################################################
func check(e error) {
  if e != nil {
    panic(e)
  }
}

//##############################################################################
//# Function: recordMetrics
//#
//# Input:   None
//# Output:  None
//#
//# Description:  This function will record all the metrics and expose them to
//#               Prometheus.  It will execute 'casadm' command to get stats
//##############################################################################
func recordMetrics_19_9() {
  go func() {
    for {
      out, err := exec.Command("bash", "-c", "casadm -P -i " + cache + " -o csv").Output()
      if (err) != nil {
        time.Sleep(time.Duration(sleepTime) * time.Second)
        continue
      }

      //remove the header which is the first line
      output := string([]byte(out))
      outArray := strings.Split(output, "\n")

      if len(outArray) < 2 {
        xprint("ERROR : data returned did not contain at least 2 lines. OUTPUT:" + fmt.Sprint(output) )
        time.Sleep(time.Duration(sleepTime) * time.Second)
        continue
      }
      headerless := outArray[1:]
      ocf_csv_data := strings.Join(headerless, "\n")

      xprint("CAS DATA:\n" + fmt.Sprint(string(ocf_csv_data)))

      parsed_ocf_data := strings.Split(ocf_csv_data, ",")

      if s,err := strconv.ParseFloat(parsed_ocf_data[headerMap[occupancy_blk]]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"usage",    "subcategory":         "occupancy"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[18]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"usage",    "subcategory":              "free"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[20]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"usage",    "subcategory":             "clean"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[22]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"usage",    "subcategory":             "dirty"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[24]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":           "rd_hits"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[26]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory": "rd_partial_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[28]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":    "rd_full_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[30]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":          "rd_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[32]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":           "wr_hits"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[34]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory": "wr_partial_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[36]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":    "wr_full_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[38]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":          "wr_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[31]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":             "rd_pt"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[39]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":             "wr_pt"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[44]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":          "serviced"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[46]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":             "total"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[48]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":    "core_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[50]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":    "core_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[52]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory": "core_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[54]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":   "cache_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[56]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":   "cache_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[58]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":"cache_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[60]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":         "volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[62]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":         "volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[64]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":      "volume_total"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[66]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":   "cache_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[68]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":   "cache_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[70]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":"cache_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[72]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":    "core_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[74]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":    "core_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[76]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory": "core_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[78]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":             "total"}).Set(s)}


      if s,err := strconv.ParseFloat(parsed_ocf_data[17]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"usage",    "subcategory":"occupancy"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[19]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"usage",    "subcategory":"free"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[21]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"usage",    "subcategory":"clean"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[23]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"usage",    "subcategory":"dirty"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[25]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_hits"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[27]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_partial_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[29]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_full_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[31]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[33]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_hits"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[35]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_partial_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[37]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_full_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[39]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[31]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_pt"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[39]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_pt"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[45]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"serviced"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[47]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"total"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[49]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"core_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[51]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"core_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[53]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"core_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[55]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"cache_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[57]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"cache_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[59]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"cache_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[61]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[63]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[65]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"volume_total"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[67]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"cache_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[69]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"cache_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[71]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"cache_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[73]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"core_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[75]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"core_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[77]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"core_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[79]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"total"}).Set(s)}

      time.Sleep(time.Duration(sleepTime) * time.Second)
    }
  }()
}

func recordMetrics_19_3() {
  go func() {
    for {
      out, err := exec.Command("bash", "-c", "casadm -P -i " + cache + " -o csv").Output()
      if (err) != nil {
        time.Sleep(time.Duration(sleepTime) * time.Second)
        continue
      }

      //remove the header which is the first line
      output := string([]byte(out))
      outArray := strings.Split(output, "\n")

      if len(outArray) < 2 {
        xprint("ERROR : data returned did not contain enough lines. OUTPUT:" + fmt.Sprint(string(output)) )
        time.Sleep(time.Duration(sleepTime) * time.Second)
        continue
      }

      //outArray := output.index('\n')
      headerless := outArray[2:]
      ocf_csv_data := strings.Join(headerless, "\n")

      //ocf_csv_data := string([]byte(output))
      xprint("CAS DATA:\n" + fmt.Sprint(string(ocf_csv_data)))

      parsed_ocf_data := strings.Split(ocf_csv_data, ",")

      if s,err := strconv.ParseFloat(parsed_ocf_data[15]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"usage",    "subcategory":         "occupancy"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[17]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"usage",    "subcategory":              "free"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[19]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"usage",    "subcategory":             "clean"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[21]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"usage",    "subcategory":             "dirty"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[23]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":           "rd_hits"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[25]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory": "rd_partial_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[27]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":    "rd_full_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[29]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":          "rd_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[31]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":           "wr_hits"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[33]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory": "wr_partial_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[35]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":    "wr_full_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[37]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":          "wr_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[30]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":             "rd_pt"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[38]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":             "wr_pt"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[43]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":          "serviced"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[45]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"requests", "subcategory":             "total"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[47]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":    "core_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[49]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":    "core_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[51]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory": "core_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[53]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":   "cache_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[55]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":   "cache_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[57]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":"cache_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[59]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":         "volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[61]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":         "volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[63]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"blocks",   "subcategory":      "volume_total"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[65]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":   "cache_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[67]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":   "cache_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[69]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":"cache_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[71]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":    "core_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[73]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":    "core_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[75]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory": "core_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[77]  ,64); err == nil { OCFStat_count.With(prometheus.Labels{"category":"errors",   "subcategory":             "total"}).Set(s)}


      if s,err := strconv.ParseFloat(parsed_ocf_data[16]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"usage",    "subcategory":"occupancy"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[18]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"usage",    "subcategory":"free"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[20]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"usage",    "subcategory":"clean"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[22]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"usage",    "subcategory":"dirty"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[24]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_hits"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[26]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_partial_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[28]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_full_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[30]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[32]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_hits"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[34]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_partial_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[36]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_full_misses"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[38]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[30]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"rd_pt"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[38]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"wr_pt"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[44]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"serviced"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[46]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"requests", "subcategory":"total"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[48]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"core_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[50]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"core_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[52]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"core_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[54]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"cache_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[56]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"cache_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[58]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"cache_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[60]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[62]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[64]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"blocks",   "subcategory":"volume_total"}).Set(s)}

      if s,err := strconv.ParseFloat(parsed_ocf_data[66]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"cache_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[68]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"cache_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[70]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"cache_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[72]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"core_volume_rd"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[74]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"core_volume_wr"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[76]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"core_volume_total"}).Set(s)}
      if s,err := strconv.ParseFloat(parsed_ocf_data[78]  ,64); err == nil { OCFStat_percentage.With(prometheus.Labels{"category":"errors",   "subcategory":"total"}).Set(s)}

      time.Sleep(time.Duration(sleepTime) * time.Second)
    }
  }()
}

//##############################################################################
//# Function: init()
//#
//# Input:   None
//# Output:  None
//#
//# Description:  This function registers all the metrics in Prometheus
//##############################################################################
func init() {
  prometheus.MustRegister(OCFStat_count)
  prometheus.MustRegister(OCFStat_percentage)
}


//##############################################################################
//# Function: xprint
//#
//# Input:   message - the string to write in a file line
//# Output:  FILE - logFile - The file to store the message
//#
//# Description:  This function will write a line to the log file if logging is
//#               enabled
//##############################################################################
func xprint( message string){
  // Max log file size in bytes
  var maxFileSize int64 = 104857600
  var didTrim bool = false

  if (isLogEnabled == false){
    return
  }

 // rotate the log after Max has been reached
   fileStat, err := os.Stat(logPath)
   if err == nil  {
     if fileStat.Size() > maxFileSize {
       err = os.Rename(logPath, logPath + ".old")
       if err != nil {
         fmt.Println("Failed to rename file")
         fmt.Println(err)
       }else {
         didTrim = true
       }
     }
   }

  // get the time and append the message to this
    currentTime := time.Now()
    formattedMsg := fmt.Sprintln(message)
    line := "MSG : " + currentTime.Format("01/02/2006 3:4:5 PM") + " - " + formattedMsg

    f, file_err := os.OpenFile(logPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)

  	if file_err != nil {
      fmt.Println("Failed to write to log file")
  		fmt.Println(file_err)
      return
  	}
  	defer f.Close()
    if didTrim {
      fmt.Fprintf(f, "Old Files have been moved to " + logPath + ".old \n")
    }
  	fmt.Fprintf(f, "%s\n", line)
}

//##############################################################################
//#  MAIN STARTS HERE
//##############################################################################
func main() {
  //argument functions, default values, help text
  portPtr := flag.Int("port", 2114, "The port number to provide metrics to")
  sleepPtr := flag.Int("sleep", 1, "The number of seconds to sleep in between metrics")
  logPtr := flag.Bool("log", false, "Turns on logging information")
  logPathPtr := flag.String("logfile", "/tmp/cas_exporter.out", "log file location")
  cachePtr := flag.String("cache", "1", "Cache Instance Number")

  flag.Parse()

  portNumber = *portPtr
  sleepTime = *sleepPtr
  isLogEnabled = *logPtr
  logPath = *logPathPtr
  cache = *cachePtr

  port := ":" + strconv.Itoa(portNumber)

  xprint("### Starting Execution of cas_exporter...")
  xprint("Port          :" + strconv.Itoa(portNumber))
  xprint("Sleep Time    :" + strconv.Itoa(sleepTime))
  xprint("isLogEnabled  :" + strconv.FormatBool(isLogEnabled))
  xprint("Log Path      :" + logPath)
  xprint("Cache Instance:" + cache)
  xprint("Other Args    :" + fmt.Sprintln(flag.Args()))

  // Test that RPC is working fail if not
  output,err := exec.Command("bash", "-c", "casadm -P -i " + cache + " -o csv").Output()
  if (err) != nil {
    fmt.Println("ERROR: Unable to start because the command [casadm -P -i " + cache + " -o csv]")
    fmt.Println("ERROR: Please ensure you have configured Open CAS Linux and that this command succeeds")
    fmt.Println(err)
    os.Exit(1)
  }
  ocf_csv_data := string([]byte(output))
  xprint("INITIAL DATA:\n" + fmt.Sprint(string(ocf_csv_data)))

  outArray := strings.Split(ocf_csv_data, "\n")

  if len(outArray) < 1 {
    xprint("ERROR : data returned did not contain at least 2 lines. OUTPUT:" + fmt.Sprint(ocf_csv_data) )
  }
  headerline := outArray[0]

  initializeHeaders()
  rc := mapHeaders(headerline)
  if rc != 0 {
    xprint("ERROR : Failed to map header" )
  }

 recordMetrics_19_9()

 http.Handle("/metrics", promhttp.Handler())
 log.Fatal(http.ListenAndServe(port, nil))
}