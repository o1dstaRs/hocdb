package hocdb_test

import (
	"errors"
	"hocdb"
	"math"
	"os"
	"testing"
)

// Deterministic OHLCV data set shared by the indicator tests (same generator
// as bindings/c/test/test_indicators.c).
const indicatorBars = 3000

var indicatorSchema = []hocdb.Field{
	{Name: "timestamp", Type: hocdb.TypeI64},
	{Name: "open", Type: hocdb.TypeF64},
	{Name: "high", Type: hocdb.TypeF64},
	{Name: "low", Type: hocdb.TypeF64},
	{Name: "close", Type: hocdb.TypeF64},
	{Name: "volume", Type: hocdb.TypeF64},
}

func indicatorLCG(s *uint64) float64 {
	*s = *s*6364136223846793005 + 1442695040888963407
	return float64(*s>>11) / 9007199254740992.0
}

// buildIndicatorDB appends 3000 pseudo-random-walk bars (timestamps 1000 + i*60)
// and returns the database together with the close series.
func buildIndicatorDB(t *testing.T, dir string) (*hocdb.DB, []float64) {
	t.Helper()

	db, err := hocdb.New("IND", dir, indicatorSchema, hocdb.Options{})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	closes := make([]float64, 0, indicatorBars)
	seed := uint64(7)
	p := 100.0
	for i := 0; i < indicatorBars; i++ {
		o := p
		p *= math.Exp((indicatorLCG(&seed) - 0.5) * 0.02)
		record, err := hocdb.CreateRecordBytes(indicatorSchema,
			int64(1000+i*60), o, math.Max(o, p)*1.003, math.Min(o, p)*0.997, p, 1000.0+float64(i%50))
		if err != nil {
			t.Fatalf("Failed to create record: %v", err)
		}
		if err := db.Append(record); err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
		closes = append(closes, p)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	return db, closes
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func TestIndicators(t *testing.T) {
	testDir := "../../../b_go_test_indicators"
	os.RemoveAll(testDir)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	db, closes := buildIndicatorDB(t, testDir)
	defer db.Close()

	lastTs := int64(1000 + (indicatorBars-1)*60)
	startTs := int64(1000 + 1000*60)
	endTs := int64(1000 + 1500*60)

	specs := []hocdb.IndicatorSpec{
		{Kind: "sma", Period: 20},
		{Kind: "macd"},
		{Kind: "rsi", Period: 14},
		{Kind: "bbands"},
		{Kind: "atr", Period: 14},
		{Kind: "obv"},
		{Kind: "sma", Period: 10, Field: "volume"},
	}

	t.Run("Batch", func(t *testing.T) {
		res, err := db.Indicators(specs, startTs, endTs, nil)
		if err != nil {
			t.Fatalf("Indicators failed: %v", err)
		}
		if res.NRows != 500 || len(res.Timestamps) != 500 {
			t.Fatalf("Expected 500 rows, got NRows=%d len(Timestamps)=%d", res.NRows, len(res.Timestamps))
		}
		if res.Timestamps[0] != startTs {
			t.Errorf("Expected first timestamp %d, got %d", startTs, res.Timestamps[0])
		}
		if res.Timestamps[499] != endTs-60 {
			t.Errorf("Expected last timestamp %d, got %d", endTs-60, res.Timestamps[499])
		}

		expected := []string{
			"sma_20",
			"macd", "macd_signal", "macd_hist",
			"rsi_14",
			"bbands_upper", "bbands_middle", "bbands_lower", "bbands_percent_b", "bbands_bandwidth",
			"atr_14",
			"obv",
			"sma_10",
		}
		if len(res.Names) != len(expected) {
			t.Fatalf("Expected %d output names, got %d: %v", len(expected), len(res.Names), res.Names)
		}
		for i, name := range expected {
			if res.Names[i] != name {
				t.Errorf("Expected output %d to be %q, got %q", i, name, res.Names[i])
			}
			col, ok := res.Columns[name]
			if !ok {
				t.Fatalf("Missing column %q", name)
			}
			if len(col) != 500 {
				t.Errorf("Column %q has %d values, expected 500", name, len(col))
			}
		}

		sma := res.Columns["sma_20"]
		rsi := res.Columns["rsi_14"]
		upper, middle, lower := res.Columns["bbands_upper"], res.Columns["bbands_middle"], res.Columns["bbands_lower"]
		atr := res.Columns["atr_14"]
		for i := 0; i < res.NRows; i++ {
			if math.IsNaN(sma[i]) {
				t.Fatalf("sma_20 row %d is NaN with automatic lookback", i)
			}
			if rsi[i] < 0 || rsi[i] > 100 || math.IsNaN(rsi[i]) {
				t.Fatalf("rsi_14 row %d out of [0,100]: %v", i, rsi[i])
			}
			if !(upper[i] >= middle[i] && middle[i] >= lower[i]) {
				t.Fatalf("bbands row %d not ordered: %v %v %v", i, upper[i], middle[i], lower[i])
			}
			if !(atr[i] > 0) {
				t.Fatalf("atr_14 row %d not positive: %v", i, atr[i])
			}
		}
	})

	t.Run("LookbackZero", func(t *testing.T) {
		res, err := db.Indicators(specs[:1], startTs, endTs, &hocdb.IndicatorOptions{Lookback: hocdb.Lookback(0)})
		if err != nil {
			t.Fatalf("Indicators failed: %v", err)
		}
		if res.NRows != 500 {
			t.Fatalf("Expected 500 rows, got %d", res.NRows)
		}
		sma := res.Columns["sma_20"]
		for i := 0; i < 19; i++ {
			if !math.IsNaN(sma[i]) {
				t.Errorf("Expected NaN warm-up at row %d, got %v", i, sma[i])
			}
		}
		if math.IsNaN(sma[19]) {
			t.Errorf("Expected row 19 to be defined with lookback 0")
		}
	})

	t.Run("Reference", func(t *testing.T) {
		res, err := db.Indicators(specs[:1], startTs, endTs, nil)
		if err != nil {
			t.Fatalf("Indicators failed: %v", err)
		}
		sma := res.Columns["sma_20"]
		for _, row := range []int{0, 19, 100, 250, 499} {
			g := 1000 + row // global record index of this row
			sum := 0.0
			for _, c := range closes[g-19 : g+1] {
				sum += c
			}
			want := sum / 20
			if rel := math.Abs(sma[row]-want) / math.Abs(want); rel > 1e-9 {
				t.Errorf("sma_20 row %d = %v, reference %v (rel err %g)", row, sma[row], want, rel)
			}
		}
	})

	t.Run("Tail", func(t *testing.T) {
		res, err := db.IndicatorsTail(5, specs[:3], nil)
		if err != nil {
			t.Fatalf("IndicatorsTail failed: %v", err)
		}
		if res.NRows != 5 || len(res.Names) != 5 {
			t.Fatalf("Expected 5 rows and 5 outputs, got %d rows, %d outputs", res.NRows, len(res.Names))
		}
		if res.Timestamps[4] != lastTs {
			t.Errorf("Expected tail to end at %d, got %d", lastTs, res.Timestamps[4])
		}
	})

	t.Run("TailBucket", func(t *testing.T) {
		res, err := db.IndicatorsTail(10, specs[:1], &hocdb.IndicatorOptions{Bucket: 300, Lookback: hocdb.Lookback(0)})
		if err != nil {
			t.Fatalf("IndicatorsTail with bucket failed: %v", err)
		}
		if res.NRows != 10 {
			t.Fatalf("Expected 10 bars, got %d", res.NRows)
		}
		for i := 1; i < res.NRows; i++ {
			if res.Timestamps[i]-res.Timestamps[i-1] != 300 {
				t.Errorf("Bars %d/%d are %d apart, expected 300", i-1, i, res.Timestamps[i]-res.Timestamps[i-1])
			}
		}
		if res.Timestamps[0]%300 != 0 {
			t.Errorf("Bar timestamp %d not aligned to 300", res.Timestamps[0])
		}
	})

	t.Run("Errors", func(t *testing.T) {
		cases := []struct {
			name  string
			specs []hocdb.IndicatorSpec
			opts  *hocdb.IndicatorOptions
		}{
			{"unknown kind", []hocdb.IndicatorSpec{{Kind: "nope"}}, nil},
			{"missing close column", specs[:1], &hocdb.IndicatorOptions{Columns: &hocdb.IndicatorColumns{Open: "open"}}},
			{"atr with only close", []hocdb.IndicatorSpec{{Kind: "atr", Period: 14}}, &hocdb.IndicatorOptions{Columns: &hocdb.IndicatorColumns{Close: "close"}}},
			{"invalid field name", []hocdb.IndicatorSpec{{Kind: "sma", Period: 5, Field: "nope"}}, nil},
			{"invalid column field name", specs[:1], &hocdb.IndicatorOptions{Columns: &hocdb.IndicatorColumns{Close: "nope"}}},
			{"field override with bucket", []hocdb.IndicatorSpec{{Kind: "sma", Period: 10, Field: "volume"}}, &hocdb.IndicatorOptions{Bucket: 300}},
			{"no specs", nil, nil},
			{"duplicate output name", []hocdb.IndicatorSpec{{Kind: "sma", Period: 20}, {Kind: "sma", Period: 20, Field: "volume"}}, nil},
		}
		for _, c := range cases {
			if _, err := db.IndicatorsTail(10, c.specs, c.opts); err == nil {
				t.Errorf("%s: expected an error", c.name)
			} else {
				t.Logf("%s -> %v", c.name, err)
			}
		}
		if _, err := db.OHLCV(math.MinInt64, math.MaxInt64, 300, "nope", ""); err == nil {
			t.Error("OHLCV with unknown price field: expected an error")
		}
		if _, err := db.Summary(math.MinInt64, math.MaxInt64, "nope", 0); err == nil {
			t.Error("Summary with unknown field: expected an error")
		}
	})

	t.Run("OHLCV", func(t *testing.T) {
		bars, err := db.OHLCV(math.MinInt64, math.MaxInt64, 300, "close", "volume")
		if err != nil {
			t.Fatalf("OHLCV failed: %v", err)
		}
		n := len(bars.Timestamps)
		if n <= 500 {
			t.Fatalf("Expected more than 500 bars, got %d", n)
		}
		if len(bars.Open) != n || len(bars.High) != n || len(bars.Low) != n || len(bars.Close) != n || len(bars.Volume) != n || len(bars.Count) != n {
			t.Fatalf("Bar slices have inconsistent lengths")
		}
		for i := 0; i < n; i++ {
			if bars.High[i] < bars.Low[i] {
				t.Fatalf("Bar %d: high %v < low %v", i, bars.High[i], bars.Low[i])
			}
			if bars.Close[i] > bars.High[i] || bars.Close[i] < bars.Low[i] {
				t.Fatalf("Bar %d: close %v outside [%v, %v]", i, bars.Close[i], bars.Low[i], bars.High[i])
			}
			if bars.Count[i] < 1 {
				t.Fatalf("Bar %d: count %v < 1", i, bars.Count[i])
			}
		}
	})

	t.Run("Summary", func(t *testing.T) {
		sum, err := db.Summary(math.MinInt64, math.MaxInt64, "close", 252)
		if err != nil {
			t.Fatalf("Summary failed: %v", err)
		}
		if len(sum) != 29 {
			t.Errorf("Expected 29 summary fields, got %d", len(sum))
		}
		if sum["count"] != indicatorBars {
			t.Errorf("Expected count %d, got %v", indicatorBars, sum["count"])
		}
		if dd := sum["max_drawdown"]; dd < -1 || dd > 0 {
			t.Errorf("max_drawdown %v outside [-1, 0]", dd)
		}
		if wr := sum["win_rate"]; wr < 0 || wr > 1 {
			t.Errorf("win_rate %v outside [0, 1]", wr)
		}
		if !finite(sum["sharpe"]) {
			t.Errorf("sharpe is not finite: %v", sum["sharpe"])
		}
	})

	t.Run("Snapshot", func(t *testing.T) {
		snap, err := db.Snapshot(nil)
		if err != nil {
			t.Fatalf("Snapshot failed: %v", err)
		}
		if len(snap.Fields) < 90 {
			t.Errorf("Expected at least 90 snapshot fields, got %d", len(snap.Fields))
		}
		if snap.Bars != 2500 {
			t.Errorf("Expected 2500 bars, got %d", snap.Bars)
		}
		if snap.Timestamp != lastTs {
			t.Errorf("Expected timestamp %d, got %d", lastTs, snap.Timestamp)
		}
		if rsi := snap.Fields["rsi_14"]; rsi < 0 || rsi > 100 || math.IsNaN(rsi) {
			t.Errorf("rsi_14 %v outside [0, 100]", rsi)
		}
		for _, name := range []string{"ema_200", "adx_14", "mfi_14", "supertrend"} {
			if v, ok := snap.Fields[name]; !ok || !finite(v) {
				t.Errorf("Snapshot field %q missing or not finite: %v", name, v)
			}
		}
		if math.Abs(snap.Fields["close"]-closes[len(closes)-1]) > 1e-9 {
			t.Errorf("Snapshot close %v != last close %v", snap.Fields["close"], closes[len(closes)-1])
		}
	})

	t.Run("SnapshotBucket", func(t *testing.T) {
		snap, err := db.Snapshot(&hocdb.SnapshotOptions{Bars: 50, Bucket: 300, PeriodsPerYear: 252})
		if err != nil {
			t.Fatalf("Snapshot with bucket failed: %v", err)
		}
		if snap.Bars != 50 {
			t.Errorf("Expected 50 bars, got %d", snap.Bars)
		}
		if !math.IsNaN(snap.Fields["sma_200"]) {
			t.Errorf("Expected sma_200 to be NaN with 50 bars, got %v", snap.Fields["sma_200"])
		}
		if !finite(snap.Fields["sma_20"]) {
			t.Errorf("Expected sma_20 to be finite, got %v", snap.Fields["sma_20"])
		}
	})

	t.Run("PriceAsClose", func(t *testing.T) {
		schema := []hocdb.Field{
			{Name: "timestamp", Type: hocdb.TypeI64},
			{Name: "price", Type: hocdb.TypeF64},
		}
		pdb, err := hocdb.New("IND_PRICE", testDir, schema, hocdb.Options{})
		if err != nil {
			t.Fatalf("Failed to create DB: %v", err)
		}
		defer pdb.Close()
		for i := 0; i < 30; i++ {
			record, _ := hocdb.CreateRecordBytes(schema, int64(i+1), 100.0+float64(i))
			if err := pdb.Append(record); err != nil {
				t.Fatalf("Failed to append: %v", err)
			}
		}
		pdb.Flush()

		res, err := pdb.IndicatorsTail(5, []hocdb.IndicatorSpec{{Kind: "sma", Period: 3}}, nil)
		if err != nil {
			t.Fatalf("Expected \"price\" to be auto-detected as close: %v", err)
		}
		if res.NRows != 5 {
			t.Fatalf("Expected 5 rows, got %d", res.NRows)
		}
		// prices are 100..129, so the last sma_3 is the mean of 127, 128, 129
		if got := res.Columns["sma_3"][4]; math.Abs(got-128) > 1e-9 {
			t.Errorf("Expected last sma_3 to be 128, got %v", got)
		}
	})
}

func TestIndicatorRegistry(t *testing.T) {
	kinds := hocdb.IndicatorKinds()
	if len(kinds) != 83 {
		t.Errorf("Expected 83 indicator kinds, got %d", len(kinds))
	}
	seen := map[string]bool{}
	for _, k := range kinds {
		seen[k] = true
	}
	for _, want := range []string{"sma", "rsi", "macd", "bbands", "heikin_ashi",
		"spread", "order_flow", "tick_pressure", "trade_intensity", "amihud", "realized_vol",
		"series", "series2", "ratio", "ratio_zscore", "rel_strength",
		"forward_return", "triple_barrier",
		"session_vwap", "session_range", "opening_range", "pivots"} {
		if !seen[want] {
			t.Errorf("Expected kind %q in IndicatorKinds", want)
		}
	}

	for kind, want := range map[string]bool{"forward_return": true, "triple_barrier": true, "sma": false, "order_flow": false, "nope": false} {
		if got := hocdb.IndicatorIsLookahead(kind); got != want {
			t.Errorf("IndicatorIsLookahead(%q) = %v, want %v", kind, got, want)
		}
	}
	if outs := hocdb.IndicatorOutputs("pivots"); len(outs) != 5 || outs[0] != "pp" {
		t.Errorf("Unexpected pivots outputs: %v", outs)
	}
	if outs := hocdb.IndicatorOutputs("order_flow"); len(outs) != 2 || outs[0] != "net" || outs[1] != "imbalance" {
		t.Errorf("Unexpected order_flow outputs: %v", outs)
	}

	if outs := hocdb.IndicatorOutputs("macd"); len(outs) != 3 || outs[0] != "macd" || outs[1] != "signal" || outs[2] != "hist" {
		t.Errorf("Unexpected MACD outputs: %v", outs)
	}
	if outs := hocdb.IndicatorOutputs("MACD"); len(outs) != 3 {
		t.Errorf("Expected kind lookup to be case-insensitive, got %v", outs)
	}
	if outs := hocdb.IndicatorOutputs("sma"); len(outs) != 1 || outs[0] != "value" {
		t.Errorf("Unexpected SMA outputs: %v", outs)
	}
	if outs := hocdb.IndicatorOutputs("nope"); outs != nil {
		t.Errorf("Expected nil outputs for an unknown kind, got %v", outs)
	}

	if w := hocdb.IndicatorWarmup(hocdb.IndicatorSpec{Kind: "ema", Period: 200}); w <= 200 {
		t.Errorf("Expected EMA(200) warm-up > 200, got %d", w)
	}
	if w := hocdb.IndicatorWarmup(hocdb.IndicatorSpec{Kind: "nope"}); w != 0 {
		t.Errorf("Expected warm-up 0 for an unknown kind, got %d", w)
	}
}

// ---------------------------------------------------------------------------
// Tick databases: microstructure, pairs, labels, sessions, health, evaluation
// ---------------------------------------------------------------------------

const tickCount = 6000

var tickSchema = []hocdb.Field{
	{Name: "timestamp", Type: hocdb.TypeI64},
	{Name: "price", Type: hocdb.TypeF64},
	{Name: "size", Type: hocdb.TypeF64},
	{Name: "bid", Type: hocdb.TypeF64},
	{Name: "ask", Type: hocdb.TypeF64},
	{Name: "side", Type: hocdb.TypeBool},
}

// buildTickDBs creates tick database A (one trade per second at 1_000_000*i,
// microseconds) and B (one trade every 2 seconds at 1_000_000*i + 300_000)
// with deterministic random-walk prices, like bindings/c/test/test_indicators.c.
func buildTickDBs(t *testing.T, dir string) (a, b *hocdb.DB) {
	t.Helper()

	a, err := hocdb.New("PAIR_A", dir, tickSchema, hocdb.Options{})
	if err != nil {
		t.Fatalf("Failed to create tick DB A: %v", err)
	}
	b, err = hocdb.New("PAIR_B", dir, tickSchema, hocdb.Options{})
	if err != nil {
		a.Close()
		t.Fatalf("Failed to create tick DB B: %v", err)
	}

	seed := uint64(11)
	pa, pb := 100.0, 50.0
	for i := 0; i < tickCount; i++ {
		pa *= math.Exp((indicatorLCG(&seed) - 0.5) * 0.004)
		pb *= math.Exp((indicatorLCG(&seed) - 0.5) * 0.004)
		rec, err := hocdb.CreateRecordBytes(tickSchema, int64(1_000_000*i), pa, 1.0+float64(i%4), pa*0.999, pa*1.001, i%3 != 0)
		if err != nil {
			t.Fatalf("Failed to create tick %d: %v", i, err)
		}
		if err := a.Append(rec); err != nil {
			t.Fatalf("Failed to append tick %d to A: %v", i, err)
		}
		if i%2 == 0 {
			rec, err := hocdb.CreateRecordBytes(tickSchema, int64(1_000_000*i+300_000), pb, 2.0, pb*0.999, pb*1.001, i%4 == 0)
			if err != nil {
				t.Fatalf("Failed to create tick %d for B: %v", i, err)
			}
			if err := b.Append(rec); err != nil {
				t.Fatalf("Failed to append tick %d to B: %v", i, err)
			}
		}
	}
	if err := a.Flush(); err != nil {
		t.Fatalf("Failed to flush A: %v", err)
	}
	if err := b.Flush(); err != nil {
		t.Fatalf("Failed to flush B: %v", err)
	}
	return a, b
}

func TestIndicatorsTicks(t *testing.T) {
	testDir := "../../../b_go_test_indicators/ticks"
	os.RemoveAll(testDir)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll("../../../b_go_test_indicators")

	a, b := buildTickDBs(t, testDir)
	defer a.Close()
	defer b.Close()

	const sec = int64(1_000_000)
	lastTs := sec * int64(tickCount-1)

	t.Run("Microstructure", func(t *testing.T) {
		specs := []hocdb.IndicatorSpec{
			{Kind: "spread"},
			{Kind: "order_flow", Period: 10},
			{Kind: "trade_intensity", Period: 10, Param: 1e6},
			{Kind: "tick_pressure", Period: 20},
			{Kind: "session_vwap", Param: 600e6}, // 10-minute sessions
			{Kind: "forward_return", Period: 5},
		}
		res, err := a.IndicatorsTail(100, specs, nil) // nil columns: price/size/bid/ask/side auto-detected
		if err != nil {
			t.Fatalf("IndicatorsTail on ticks failed: %v", err)
		}
		if res.NRows != 100 {
			t.Fatalf("Expected 100 rows, got %d", res.NRows)
		}
		expected := []string{
			"spread_abs", "spread_bps",
			"order_flow_10_net", "order_flow_10_imbalance",
			"trade_intensity_10_trades_per_sec", "trade_intensity_10_volume_per_sec",
			"tick_pressure_20",
			"session_vwap",
			"forward_return_5_ret", "forward_return_5_max", "forward_return_5_min",
		}
		if len(res.Names) != len(expected) {
			t.Fatalf("Expected %d outputs, got %d: %v", len(expected), len(res.Names), res.Names)
		}
		for i, name := range expected {
			if res.Names[i] != name {
				t.Errorf("Expected output %d to be %q, got %q", i, name, res.Names[i])
			}
			if len(res.Columns[name]) != 100 {
				t.Errorf("Column %q has %d values, expected 100", name, len(res.Columns[name]))
			}
		}
		if res.Timestamps[99] != lastTs {
			t.Errorf("Expected tail to end at %d, got %d", lastTs, res.Timestamps[99])
		}
		bps := res.Columns["spread_bps"]
		tps := res.Columns["trade_intensity_10_trades_per_sec"]
		imb := res.Columns["order_flow_10_imbalance"]
		fwd := res.Columns["forward_return_5_ret"]
		for i := 0; i < 100; i++ {
			if math.Abs(bps[i]-20) > 1e-9 {
				t.Fatalf("spread_bps row %d = %v, expected 20", i, bps[i])
			}
			if math.Abs(tps[i]-1) > 1e-9 {
				t.Fatalf("trades_per_sec row %d = %v, expected 1", i, tps[i])
			}
			if math.IsNaN(imb[i]) || imb[i] < -1 || imb[i] > 1 {
				t.Fatalf("imbalance row %d = %v, outside [-1, 1]", i, imb[i])
			}
			if i >= 95 {
				if !math.IsNaN(fwd[i]) {
					t.Errorf("forward_return_5_ret row %d = %v, expected NaN (no future rows)", i, fwd[i])
				}
			} else if !finite(fwd[i]) {
				t.Errorf("forward_return_5_ret row %d = %v, expected finite", i, fwd[i])
			}
		}

		// Explicit column roles work too, and the spread needs bid + ask.
		cols := &hocdb.IndicatorColumns{Close: "price", Volume: "size", Bid: "bid", Ask: "ask", Side: "side"}
		if _, err := a.IndicatorsTail(10, specs[:2], &hocdb.IndicatorOptions{Columns: cols}); err != nil {
			t.Errorf("Explicit bid/ask/side columns failed: %v", err)
		}
		if _, err := a.IndicatorsTail(10, specs[:1], &hocdb.IndicatorOptions{Columns: &hocdb.IndicatorColumns{Close: "price"}}); err == nil {
			t.Error("spread without bid/ask columns: expected an error")
		} else {
			t.Logf("spread without bid/ask -> %v", err)
		}
		if _, err := a.IndicatorsTail(10, specs[:1], &hocdb.IndicatorOptions{Columns: &hocdb.IndicatorColumns{Close: "price", Bid: "nope", Ask: "ask"}}); err == nil {
			t.Error("unknown bid field name: expected an error")
		}
	})

	t.Run("SessionParamRequired", func(t *testing.T) {
		// Param = 0 selects the handle's trading calendar sessions; this handle
		// has no calendar / timestamp unit, so the engine answers CalendarRequired (-30).
		for _, kind := range []string{"session_vwap", "session_range", "opening_range", "pivots"} {
			if _, err := a.IndicatorsTail(10, []hocdb.IndicatorSpec{{Kind: kind}}, nil); err == nil {
				t.Errorf("%s without Param: expected a CalendarRequired error", kind)
			} else if !errors.Is(err, hocdb.ErrCalendarRequired) {
				t.Errorf("%s without Param: expected ErrCalendarRequired, got %v", kind, err)
			} else {
				t.Logf("%s without Param -> %v", kind, err)
			}
		}
		// pivots need high/low: run it on 1-minute bars (10-minute sessions)
		res, err := a.IndicatorsTail(10, []hocdb.IndicatorSpec{{Kind: "pivots", Param: 600e6}}, &hocdb.IndicatorOptions{Bucket: 60 * sec})
		if err != nil {
			t.Fatalf("pivots with Param on bars failed: %v", err)
		}
		if res.NRows != 10 || !finite(res.Columns["pivots_pp"][9]) {
			t.Errorf("pivots on bars: %d rows, last pp %v", res.NRows, res.Columns["pivots_pp"][9])
		}
		if len(res.Names) != 5 || res.Names[0] != "pivots_pp" || res.Names[4] != "pivots_s2" {
			t.Errorf("Unexpected pivots output names: %v", res.Names)
		}
	})

	t.Run("Lookahead", func(t *testing.T) {
		if !hocdb.IndicatorIsLookahead("forward_return") {
			t.Error("IndicatorIsLookahead(forward_return) = false")
		}
		if hocdb.IndicatorIsLookahead("sma") {
			t.Error("IndicatorIsLookahead(sma) = true")
		}
	})

	pairSpecs := []hocdb.IndicatorSpec{
		{Kind: "series"},
		{Kind: "series2"},
		{Kind: "ratio"},
		{Kind: "correl", Period: 30},
		{Kind: "rel_strength", Period: 10},
	}

	t.Run("PairTail", func(t *testing.T) {
		res, err := a.PairIndicatorsTail(b, 50, pairSpecs, nil)
		if err != nil {
			t.Fatalf("PairIndicatorsTail failed: %v", err)
		}
		if res.NRows != 50 || len(res.Names) != 5 {
			t.Fatalf("Expected 50 rows and 5 outputs, got %d rows, %d outputs (%v)", res.NRows, len(res.Names), res.Names)
		}
		want := []string{"series", "series2", "ratio", "correl_30", "rel_strength_10"}
		for i, name := range want {
			if res.Names[i] != name {
				t.Errorf("Expected output %d to be %q, got %q", i, name, res.Names[i])
			}
		}
		s1, s2, ratio, correl := res.Columns["series"], res.Columns["series2"], res.Columns["ratio"], res.Columns["correl_30"]
		for i := 0; i < 50; i++ {
			if math.Abs(s1[i]/s2[i]-ratio[i]) > 1e-12 {
				t.Fatalf("ratio row %d = %v, expected series/series2 = %v", i, ratio[i], s1[i]/s2[i])
			}
			if !finite(correl[i]) {
				t.Fatalf("correl_30 row %d = %v, expected finite", i, correl[i])
			}
			if s1[i] < 50 || s1[i] > 200 || s2[i] < 25 || s2[i] > 100 {
				t.Fatalf("row %d: series %v / series2 %v look wrong (A ~100, B ~50)", i, s1[i], s2[i])
			}
		}
		if res.Timestamps[49] != lastTs {
			t.Errorf("Expected pair tail to end at A's last tick %d, got %d", lastTs, res.Timestamps[49])
		}

		// The same call with explicit column roles on both sides.
		cols := &hocdb.IndicatorColumns{Close: "price", Volume: "size", Bid: "bid", Ask: "ask", Side: "side"}
		res2, err := a.PairIndicatorsTail(b, 50, pairSpecs, &hocdb.PairOptions{
			IndicatorOptions: hocdb.IndicatorOptions{Columns: cols},
			OtherColumns:     cols,
		})
		if err != nil {
			t.Fatalf("PairIndicatorsTail with explicit columns failed: %v", err)
		}
		if res2.Columns["ratio"][49] != ratio[49] {
			t.Errorf("Explicit columns gave ratio %v, auto-detected %v", res2.Columns["ratio"][49], ratio[49])
		}
		if _, err := a.PairIndicatorsTail(b, 50, pairSpecs, &hocdb.PairOptions{OtherColumns: &hocdb.IndicatorColumns{Close: "nope"}}); err == nil {
			t.Error("unknown field in OtherColumns: expected an error")
		}
		if _, err := a.PairIndicatorsTail(nil, 50, pairSpecs, nil); err == nil {
			t.Error("nil other database: expected an error")
		}
	})

	t.Run("PairRangeBars", func(t *testing.T) {
		start, end := sec*1000, sec*2000
		res, err := a.PairIndicators(b, pairSpecs, start, end, &hocdb.PairOptions{
			IndicatorOptions: hocdb.IndicatorOptions{Bucket: 10 * sec},
		})
		if err != nil {
			t.Fatalf("PairIndicators with bucket failed: %v", err)
		}
		if res.NRows != 100 {
			t.Fatalf("Expected 100 ten-second bars, got %d", res.NRows)
		}
		if res.Timestamps[0] != start {
			t.Errorf("Expected first bar at %d, got %d", start, res.Timestamps[0])
		}
		for i := 0; i < res.NRows; i++ {
			if res.Timestamps[i]%(10*sec) != 0 {
				t.Fatalf("Bar %d timestamp %d not aligned to 10 s", i, res.Timestamps[i])
			}
			if i > 0 && res.Timestamps[i]-res.Timestamps[i-1] != 10*sec {
				t.Fatalf("Bars %d/%d are %d apart, expected %d", i-1, i, res.Timestamps[i]-res.Timestamps[i-1], 10*sec)
			}
			if !finite(res.Columns["ratio"][i]) {
				t.Fatalf("ratio bar %d = %v, expected finite", i, res.Columns["ratio"][i])
			}
		}
	})

	t.Run("OHLCVSide", func(t *testing.T) {
		bars, err := a.OHLCVSide(math.MinInt64, math.MaxInt64, 60*sec, "price", "size", "side")
		if err != nil {
			t.Fatalf("OHLCVSide failed: %v", err)
		}
		if n := len(bars.Timestamps); n != 100 {
			t.Fatalf("Expected 100 one-minute bars, got %d", n)
		}
		if bars.BuyVolume == nil || len(bars.BuyVolume) != 100 {
			t.Fatalf("Expected BuyVolume with 100 entries, got %v", bars.BuyVolume)
		}
		for i := range bars.Timestamps {
			if bars.BuyVolume[i] < 0 || bars.BuyVolume[i] > bars.Volume[i] {
				t.Fatalf("Bar %d: buy volume %v outside [0, %v]", i, bars.BuyVolume[i], bars.Volume[i])
			}
			if bars.Count[i] != 60 {
				t.Fatalf("Bar %d: expected 60 ticks, got %v", i, bars.Count[i])
			}
		}
		// two of every three ticks are buys; sizes cycle 1,2,3,4 so buy volume is ~2/3 of the volume
		if frac := bars.BuyVolume[1] / bars.Volume[1]; frac < 0.55 || frac > 0.8 {
			t.Errorf("Bar 1 buy fraction %v, expected about 2/3", frac)
		}

		plain, err := a.OHLCV(math.MinInt64, math.MaxInt64, 60*sec, "price", "size")
		if err != nil {
			t.Fatalf("OHLCV failed: %v", err)
		}
		if plain.BuyVolume != nil {
			t.Errorf("OHLCV without side: expected nil BuyVolume, got %d entries", len(plain.BuyVolume))
		}
		if len(plain.Timestamps) != 100 || plain.Close[99] != bars.Close[99] || plain.Volume[5] != bars.Volume[5] {
			t.Errorf("OHLCV and OHLCVSide disagree on the bars")
		}
		if _, err := a.OHLCVSide(math.MinInt64, math.MaxInt64, 60*sec, "price", "size", "nope"); err == nil {
			t.Error("OHLCVSide with unknown side field: expected an error")
		}
	})

	t.Run("Health", func(t *testing.T) {
		h, err := a.Health(math.MinInt64, math.MaxInt64, "price", "size", 5*sec, 0.05)
		if err != nil {
			t.Fatalf("Health failed: %v", err)
		}
		if len(h) != 19 {
			t.Errorf("Expected 19 health fields, got %d: %v", len(h), h)
		}
		want := map[string]float64{
			"count":               tickCount,
			"n_gaps":              0,
			"median_gap":          1_000_000,
			"n_outlier_returns":   0,
			"first_ts":            0,
			"last_ts":             5_999_000_000,
			"n_nonpositive_price": 0,
			"n_zero_volume":       0,
			// calendar statistics: 0 on a handle without a trading calendar
			"closed_span":        0,
			"n_session_breaks":   0,
			"n_missing_sessions": 0,
		}
		for name, v := range want {
			got, ok := h[name]
			if !ok {
				t.Errorf("Health field %q missing", name)
			} else if got != v {
				t.Errorf("Health %s = %v, expected %v", name, got, v)
			}
		}
		if _, err := a.Health(math.MinInt64, math.MaxInt64, "price", "", 0, 0); err != nil {
			t.Errorf("Health without volume failed: %v", err)
		}
		if _, err := a.Health(math.MinInt64, math.MaxInt64, "nope", "", 0, 0); err == nil {
			t.Error("Health with unknown price field: expected an error")
		}
	})

	t.Run("Evaluate", func(t *testing.T) {
		decisions := []hocdb.Decision{
			{Timestamp: 100 * sec, Direction: 1, Size: 1000, Horizon: 60 * sec},
			{Timestamp: 200 * sec, Direction: -1, Size: 500},                    // Horizon 0 -> default horizon
			{Timestamp: 5990 * sec, Direction: 1, Size: 100, Horizon: 60 * sec}, // exit beyond the data
			{Timestamp: 300 * sec, Direction: 0},                                // flat
		}
		ev, err := a.Evaluate(decisions, "price", 120*sec, 5)
		if err != nil {
			t.Fatalf("Evaluate failed: %v", err)
		}
		if len(ev.Fields) != 20 {
			t.Errorf("Expected 20 evaluation fields, got %d: %v", len(ev.Fields), ev.Fields)
		}
		for name, v := range map[string]float64{"n_decisions": 4, "n_evaluated": 2, "n_long": 2, "n_short": 1} {
			if ev.Fields[name] != v {
				t.Errorf("Evaluation %s = %v, expected %v", name, ev.Fields[name], v)
			}
		}
		if len(ev.Entry) != 4 || len(ev.Exit) != 4 || len(ev.NetReturn) != 4 {
			t.Fatalf("Expected 4 per-decision entries, got %d/%d/%d", len(ev.Entry), len(ev.Exit), len(ev.NetReturn))
		}
		if !finite(ev.NetReturn[0]) || !finite(ev.NetReturn[1]) {
			t.Errorf("Decisions 0 and 1 should be evaluated: net %v %v", ev.NetReturn[0], ev.NetReturn[1])
		}
		if !math.IsNaN(ev.NetReturn[2]) || !math.IsNaN(ev.NetReturn[3]) || !math.IsNaN(ev.Entry[3]) || !math.IsNaN(ev.Exit[2]) {
			t.Errorf("Decisions 2 and 3 should be NaN: entry %v exit %v net %v %v", ev.Entry[3], ev.Exit[2], ev.NetReturn[2], ev.NetReturn[3])
		}
		if want := ev.Exit[0]/ev.Entry[0] - 1 - 0.001; math.Abs(ev.NetReturn[0]-want) > 1e-12 {
			t.Errorf("net[0] = %v, expected exit/entry - 1 - 2 x 5 bps = %v", ev.NetReturn[0], want)
		}
		if !finite(ev.Fields["total_pnl"]) || ev.Fields["total_cost"] <= 0 {
			t.Errorf("total_pnl %v / total_cost %v look wrong", ev.Fields["total_pnl"], ev.Fields["total_cost"])
		}

		empty, err := a.Evaluate(nil, "price", 1, 0)
		if err != nil {
			t.Fatalf("Evaluate with no decisions failed: %v", err)
		}
		if empty.Fields["n_evaluated"] != 0 || !math.IsNaN(empty.Fields["hit_rate"]) {
			t.Errorf("Empty evaluation: n_evaluated %v hit_rate %v", empty.Fields["n_evaluated"], empty.Fields["hit_rate"])
		}
		if len(empty.Entry) != 0 || len(empty.NetReturn) != 0 {
			t.Errorf("Empty evaluation should have empty per-decision slices")
		}
		if _, err := a.Evaluate(decisions, "nope", 1, 0); err == nil {
			t.Error("Evaluate with unknown price field: expected an error")
		}
	})

	t.Run("SnapshotMulti", func(t *testing.T) {
		multi, err := a.SnapshotMulti(&hocdb.SnapshotMultiOptions{
			Buckets:        []int64{60 * sec, 300 * sec},
			PeriodsPerYear: []float64{525600, 105120},
			Bars:           50,
		})
		if err != nil {
			t.Fatalf("SnapshotMulti failed: %v", err)
		}
		if len(multi) != 2 {
			t.Fatalf("Expected 2 snapshots, got %d", len(multi))
		}
		if multi[0].Bars != 50 {
			t.Errorf("Expected 50 one-minute bars, got %d", multi[0].Bars)
		}
		if multi[1].Bars != 20 {
			t.Errorf("Expected 20 five-minute bars (100 minutes of data), got %d", multi[1].Bars)
		}
		single, err := a.Snapshot(&hocdb.SnapshotOptions{Bars: 50, Bucket: 60 * sec, PeriodsPerYear: 525600})
		if err != nil {
			t.Fatalf("Snapshot failed: %v", err)
		}
		if single.Timestamp != multi[0].Timestamp || single.Bars != multi[0].Bars {
			t.Errorf("Snapshot header differs: %d/%d vs %d/%d", single.Timestamp, single.Bars, multi[0].Timestamp, multi[0].Bars)
		}
		if len(single.Fields) != len(multi[0].Fields) {
			t.Fatalf("Snapshot has %d fields, SnapshotMulti[0] has %d", len(single.Fields), len(multi[0].Fields))
		}
		for name, v := range single.Fields {
			m, ok := multi[0].Fields[name]
			if !ok {
				t.Errorf("SnapshotMulti[0] lacks field %q", name)
				continue
			}
			if !(v == m || (math.IsNaN(v) && math.IsNaN(m))) {
				t.Errorf("Field %q: Snapshot %v != SnapshotMulti[0] %v", name, v, m)
			}
		}
		if multi[0].Timestamp == multi[1].Timestamp {
			t.Errorf("Expected different bar timestamps for 1-minute and 5-minute snapshots, both %d", multi[0].Timestamp)
		}

		if _, err := a.SnapshotMulti(nil); err == nil {
			t.Error("SnapshotMulti(nil): expected an error")
		}
		if _, err := a.SnapshotMulti(&hocdb.SnapshotMultiOptions{Buckets: []int64{60 * sec}, PeriodsPerYear: []float64{1, 2}}); err == nil {
			t.Error("SnapshotMulti with mismatched PeriodsPerYear: expected an error")
		}
		one, err := a.SnapshotMulti(&hocdb.SnapshotMultiOptions{Buckets: []int64{60 * sec}, Bars: 50})
		if err != nil || len(one) != 1 || one[0].Bars != 50 {
			t.Errorf("SnapshotMulti without PeriodsPerYear: %v (len %d)", err, len(one))
		}
	})
}
