// Go cross-binding consistency program (see ../README.md).
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"hocdb"
)

type specReq struct {
	Kind    string  `json:"kind"`
	Period  int     `json:"period"`
	Period2 int     `json:"period2"`
	Period3 int     `json:"period3"`
	Period4 int     `json:"period4"`
	Param   float64 `json:"param"`
	Param2  float64 `json:"param2"`
}

type request struct {
	Ticker   string    `json:"ticker"`
	Dir      string    `json:"dir"`
	Tail     int       `json:"tail"`
	Bucket   int64     `json:"bucket"`
	Specs    []specReq `json:"specs"`
	Snapshot struct {
		Bars           int     `json:"bars"`
		Bucket         int64   `json:"bucket"`
		PeriodsPerYear float64 `json:"periods_per_year"`
	} `json:"snapshot"`
	Summary struct {
		Start          int64   `json:"start"`
		End            int64   `json:"end"`
		Field          string  `json:"field"`
		PeriodsPerYear float64 `json:"periods_per_year"`
	} `json:"summary"`
	Ohlcv struct {
		Start  int64 `json:"start"`
		End    int64 `json:"end"`
		Bucket int64 `json:"bucket"`
	} `json:"ohlcv"`
}

func num(x float64) string {
	if math.IsNaN(x) {
		return "null"
	}
	if math.IsInf(x, 1) {
		return "\"inf\""
	}
	if math.IsInf(x, -1) {
		return "\"-inf\""
	}
	return strconv.FormatFloat(x, 'g', -1, 64)
}

func arr(a []float64) string {
	parts := make([]string, len(a))
	for i, v := range a {
		parts[i] = num(v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func ints(a []int64) string {
	parts := make([]string, len(a))
	for i, v := range a {
		parts[i] = strconv.FormatInt(v, 10)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func main() {
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		die(err)
	}
	var req request
	if err := json.Unmarshal(raw, &req); err != nil {
		die(err)
	}
	schema := []hocdb.Field{{Name: "timestamp", Type: hocdb.TypeI64}, {Name: "price", Type: hocdb.TypeF64}, {Name: "size", Type: hocdb.TypeF64},
		{Name: "bid", Type: hocdb.TypeF64}, {Name: "ask", Type: hocdb.TypeF64}, {Name: "side", Type: hocdb.TypeBool}}
	db, err := hocdb.New(req.Ticker, req.Dir, schema, hocdb.Options{})
	if err != nil {
		die(err)
	}
	defer db.Close()
	cols := &hocdb.IndicatorColumns{Close: "price", Volume: "size"}
	specs := make([]hocdb.IndicatorSpec, len(req.Specs))
	for i, s := range req.Specs {
		specs[i] = hocdb.IndicatorSpec{Kind: s.Kind, Period: s.Period, Period2: s.Period2, Period3: s.Period3, Period4: s.Period4, Param: s.Param, Param2: s.Param2}
	}
	res, err := db.IndicatorsTail(req.Tail, specs, &hocdb.IndicatorOptions{Columns: cols, Bucket: req.Bucket})
	if err != nil {
		die(err)
	}
	var sb strings.Builder
	sb.WriteString("{\"binding\":\"go\",\"timestamps\":" + ints(res.Timestamps) + ",\"columns\":{")
	for k, name := range res.Names {
		if k > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("\"" + name + "\":" + arr(res.Columns[name]))
	}
	sb.WriteString("}")
	snap, err := db.Snapshot(&hocdb.SnapshotOptions{Columns: cols, Bars: req.Snapshot.Bars, Bucket: req.Snapshot.Bucket, PeriodsPerYear: req.Snapshot.PeriodsPerYear})
	if err != nil {
		die(err)
	}
	sb.WriteString(",\"snapshot\":{\"timestamp\":" + strconv.FormatInt(snap.Timestamp, 10) + ",\"bars\":" + strconv.FormatUint(snap.Bars, 10))
	for k, v := range snap.Fields {
		sb.WriteString(",\"" + k + "\":" + num(v))
	}
	sb.WriteString("}")
	sum, err := db.Summary(req.Summary.Start, req.Summary.End, req.Summary.Field, req.Summary.PeriodsPerYear)
	if err != nil {
		die(err)
	}
	sb.WriteString(",\"summary\":{")
	first := true
	for k, v := range sum {
		if !first {
			sb.WriteString(",")
		}
		first = false
		if k == "count" {
			sb.WriteString("\"count\":" + strconv.FormatInt(int64(v), 10))
		} else {
			sb.WriteString("\"" + k + "\":" + num(v))
		}
	}
	sb.WriteString("}")
	bars, err := db.OHLCV(req.Ohlcv.Start, req.Ohlcv.End, req.Ohlcv.Bucket, "price", "size")
	if err != nil {
		die(err)
	}
	sb.WriteString(",\"ohlcv\":{\"timestamps\":" + ints(bars.Timestamps) + ",\"open\":" + arr(bars.Open) + ",\"high\":" + arr(bars.High) +
		",\"low\":" + arr(bars.Low) + ",\"close\":" + arr(bars.Close) + ",\"volume\":" + arr(bars.Volume) + ",\"count\":" + arr(bars.Count) + "}}\n")
	os.Stdout.WriteString(sb.String())
}
