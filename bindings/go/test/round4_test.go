package hocdb_test

import (
	"errors"
	"hocdb"
	"math"
	"os"
	"strings"
	"testing"
)

// Round 4: trading calendars, the signal backtester and universe features
// (mirrors bindings/c/test/test_calendar.c, test_backtest.c and
// test_universe.c). Data directory: b_go_test_round4, removed on exit.

const round4Dir = "../../../b_go_test_round4"

const usec = int64(1_000_000)

// utcAt returns UTC seconds of a civil date and time.
func utcAt(y, m, d, h, mi int) int64 {
	return hocdb.DaysFromCivil(y, m, d)*86400 + int64(h)*3600 + int64(mi)*60
}

func near(a, b, tol float64) bool { return math.Abs(a-b) <= tol }

// sameFloat treats two NaNs as equal.
func sameFloat(a, b float64) bool { return a == b || (math.IsNaN(a) && math.IsNaN(b)) }

func round4Setup(t *testing.T) {
	t.Helper()
	os.RemoveAll(round4Dir)
	if err := os.MkdirAll(round4Dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", round4Dir, err)
	}
	t.Cleanup(func() { os.RemoveAll(round4Dir) })
}

// --- A. calendars ------------------------------------------------------------

func TestRound4Calendars(t *testing.T) {
	nyse := hocdb.CalendarNYSE
	if hocdb.CalendarID("nyse") != 3 || hocdb.CalendarID("LSE") != hocdb.CalendarLSE {
		t.Errorf("CalendarID: nyse=%d LSE=%d", hocdb.CalendarID("nyse"), hocdb.CalendarID("LSE"))
	}
	if hocdb.CalendarID("nope") != 0 {
		t.Error("CalendarID(nope) != 0")
	}
	if hocdb.CalendarName(5) != "lse" || hocdb.CalendarName(hocdb.CalendarCrypto) != "crypto" {
		t.Errorf("CalendarName: 5=%q 1=%q", hocdb.CalendarName(5), hocdb.CalendarName(1))
	}
	if hocdb.CalendarName(99) != "" {
		t.Errorf("CalendarName(99) = %q, expected \"\"", hocdb.CalendarName(99))
	}

	fri := utcAt(2025, 9, 5, 15, 0) // Friday 11:00 New York
	if open, err := hocdb.CalendarIsOpen(nyse, fri); err != nil || !open {
		t.Errorf("CalendarIsOpen(nyse, Friday 15:00 UTC) = %v, %v", open, err)
	}
	s, err := hocdb.CalendarSession(nyse, fri, hocdb.SessionAt)
	if err != nil || s == nil {
		t.Fatalf("CalendarSession(nyse, Friday) = %v, %v", s, err)
	}
	if s.Open != utcAt(2025, 9, 5, 13, 30) || s.Close != utcAt(2025, 9, 5, 20, 0) || s.EarlyClose || s.TradeDay != hocdb.DaysFromCivil(2025, 9, 5) {
		t.Errorf("Friday session = %+v", *s)
	}
	sat := utcAt(2025, 9, 6, 12, 0)
	if s, err := hocdb.CalendarSession(nyse, sat, hocdb.SessionAt); err != nil || s != nil {
		t.Errorf("Saturday: expected no session, got %v, %v", s, err)
	}
	if open, err := hocdb.CalendarIsOpen(nyse, sat); err != nil || open {
		t.Errorf("CalendarIsOpen(nyse, Saturday) = %v, %v", open, err)
	}
	if s, err := hocdb.CalendarSession(nyse, sat, hocdb.SessionPrev); err != nil || s == nil || s.TradeDay != hocdb.DaysFromCivil(2025, 9, 5) {
		t.Errorf("previous session from Saturday = %v, %v", s, err)
	}
	if s, err := hocdb.CalendarSession(nyse, sat, hocdb.SessionNext); err != nil || s == nil || s.TradeDay != hocdb.DaysFromCivil(2025, 9, 8) {
		t.Errorf("next session from Saturday = %v, %v", s, err)
	}
	if _, err := hocdb.CalendarSession(nyse, sat, 7); err == nil {
		t.Error("invalid session selector: expected an error")
	}
	if s, err := hocdb.CalendarSessionForDay(nyse, hocdb.DaysFromCivil(2025, 7, 4)); err != nil || s != nil {
		t.Errorf("Independence Day: expected no session, got %v, %v", s, err)
	}
	if s, err := hocdb.CalendarSessionForDay(nyse, hocdb.DaysFromCivil(2025, 11, 28)); err != nil || s == nil || !s.EarlyClose || s.Close != utcAt(2025, 11, 28, 18, 0) {
		t.Errorf("Black Friday early close = %v, %v", s, err)
	}

	// unknown id -> ErrUnknownCalendar everywhere
	if _, err := hocdb.CalendarSession(99, fri, hocdb.SessionAt); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("CalendarSession(99): expected ErrUnknownCalendar, got %v", err)
	}
	if _, err := hocdb.CalendarSessionForDay(99, 0); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("CalendarSessionForDay(99): got %v", err)
	}
	if _, err := hocdb.CalendarIsOpen(99, fri); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("CalendarIsOpen(99): got %v", err)
	}
	if _, err := hocdb.CalendarOpenSeconds(99, 0, 1); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("CalendarOpenSeconds(99): got %v", err)
	}
	if _, err := hocdb.CalendarSessionsBetween(99, 0, 1); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("CalendarSessionsBetween(99): got %v", err)
	}
	if _, err := hocdb.CalendarPeriodsPerYear(99, 60); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("CalendarPeriodsPerYear(99): got %v", err)
	}
	if _, err := hocdb.CalendarToLocal(99, fri); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("CalendarToLocal(99): got %v", err)
	}

	if secs, err := hocdb.CalendarOpenSeconds(nyse, utcAt(2025, 8, 29, 15, 0), utcAt(2025, 9, 2, 15, 0)); err != nil || secs != 5*3600+5400 {
		t.Errorf("open seconds over Labor Day weekend = %d, %v (expected %d)", secs, err, 5*3600+5400)
	}
	if n, err := hocdb.CalendarSessionsBetween(nyse, utcAt(2025, 1, 1, 0, 0), utcAt(2026, 1, 1, 0, 0)); err != nil || n != 250 {
		t.Errorf("NYSE sessions in 2025 = %d, %v (expected 250)", n, err)
	}
	if ppy, err := hocdb.CalendarPeriodsPerYear(nyse, 60); err != nil || !near(ppy, 252*390, 1e-9) {
		t.Errorf("CalendarPeriodsPerYear(nyse, 60) = %v, %v", ppy, err)
	}
	if ppy, err := hocdb.CalendarPeriodsPerYear(hocdb.CalendarCrypto, 86400); err != nil || !near(ppy, 365, 1e-9) {
		t.Errorf("CalendarPeriodsPerYear(crypto, 86400) = %v, %v", ppy, err)
	}
	if local, err := hocdb.CalendarToLocal(nyse, utcAt(2025, 9, 5, 15, 0)); err != nil || local != utcAt(2025, 9, 5, 11, 0) {
		t.Errorf("CalendarToLocal (EDT) = %d, %v", local, err)
	}
	if y, m, d := hocdb.CivilFromDays(hocdb.DaysFromCivil(2024, 2, 29)); y != 2024 || m != 2 || d != 29 {
		t.Errorf("civil round trip = %d-%d-%d", y, m, d)
	}
	if hocdb.DaysFromCivil(1970, 1, 1) != 0 || hocdb.DaysFromCivil(2025, 13, 1) != 0 {
		t.Error("DaysFromCivil epoch / invalid month")
	}
	if s, err := hocdb.CalendarSession(hocdb.CalendarFX, utcAt(2025, 9, 7, 22, 0), hocdb.SessionAt); err != nil || s == nil || s.Open != utcAt(2025, 9, 7, 21, 0) {
		t.Errorf("fx Sunday evening session = %v, %v", s, err)
	}

	// custom calendar: Monday-Thursday 10:00-15:00 UTC+9, one holiday, one early close
	day := &hocdb.DaySession{OpenSec: 36000, CloseSec: 54000}
	weekly := [7]*hocdb.DaySession{day, day, day, day, nil, nil, nil}
	holidays := []int32{int32(hocdb.DaysFromCivil(2025, 9, 9))}
	early := []hocdb.EarlyClose{{Day: int32(hocdb.DaysFromCivil(2025, 9, 10)), CloseSec: 43200}}
	cid, err := hocdb.CalendarDefine("go_custom", weekly, 9*3600, hocdb.DstNone, holidays, early, 200)
	if err != nil || cid < 32 {
		t.Fatalf("CalendarDefine = %d, %v", cid, err)
	}
	if hocdb.CalendarID("go_custom") != cid || hocdb.CalendarName(cid) != "go_custom" {
		t.Errorf("custom calendar lookup: id %d name %q", hocdb.CalendarID("go_custom"), hocdb.CalendarName(cid))
	}
	if s, err := hocdb.CalendarSessionForDay(cid, hocdb.DaysFromCivil(2025, 9, 8)); err != nil || s == nil || s.Open != utcAt(2025, 9, 8, 1, 0) || s.Close != utcAt(2025, 9, 8, 6, 0) {
		t.Errorf("custom Monday = %v, %v", s, err)
	}
	if s, err := hocdb.CalendarSessionForDay(cid, hocdb.DaysFromCivil(2025, 9, 9)); err != nil || s != nil {
		t.Errorf("custom holiday: expected no session, got %v, %v", s, err)
	}
	if s, err := hocdb.CalendarSessionForDay(cid, hocdb.DaysFromCivil(2025, 9, 10)); err != nil || s == nil || !s.EarlyClose || s.Close != utcAt(2025, 9, 10, 3, 0) {
		t.Errorf("custom early close = %v, %v", s, err)
	}
	if s, err := hocdb.CalendarSessionForDay(cid, hocdb.DaysFromCivil(2025, 9, 12)); err != nil || s != nil {
		t.Errorf("custom Friday: expected no session, got %v, %v", s, err)
	}
	if ppy, err := hocdb.CalendarPeriodsPerYear(cid, 3600); err != nil || !near(ppy, 200*5, 1e-9) {
		t.Errorf("custom periods per year (hourly) = %v, %v", ppy, err)
	}
	if id, err := hocdb.CalendarDefine("", weekly, 0, hocdb.DstNone, nil, nil, 200); err == nil {
		t.Errorf("empty name: expected an error, got id %d", id)
	}
	if _, err := hocdb.CalendarDefine("go_bad_dst", weekly, 0, "mars", nil, nil, 200); err == nil {
		t.Error("invalid DST rule: expected an error")
	}
	if id2, err := hocdb.CalendarDefine("go_custom", weekly, 9*3600, "0", holidays, early, 200); err != nil || id2 != cid {
		t.Errorf("redefining a name should reuse its id: %d, %v (first %d)", id2, err, cid)
	}
}

// --- B. database with a calendar --------------------------------------------

var round4BarSchema = []hocdb.Field{
	{Name: "timestamp", Type: hocdb.TypeI64},
	{Name: "open", Type: hocdb.TypeF64},
	{Name: "high", Type: hocdb.TypeF64},
	{Name: "low", Type: hocdb.TypeF64},
	{Name: "close", Type: hocdb.TypeF64},
	{Name: "volume", Type: hocdb.TypeF64},
}

func TestRound4CalendarDatabase(t *testing.T) {
	round4Setup(t)
	nyse := hocdb.CalendarNYSE

	if _, err := hocdb.New("T", round4Dir, round4BarSchema, hocdb.Options{CalendarName: "nope", TimestampUnitNs: 1000}); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("unknown calendar name: expected ErrUnknownCalendar before opening, got %v", err)
	}
	if _, err := hocdb.New("T", round4Dir, round4BarSchema, hocdb.Options{Calendar: 999, TimestampUnitNs: 1000}); err == nil || !strings.Contains(err.Error(), "UnknownCalendar") {
		t.Errorf("unknown calendar id: expected an UnknownCalendar error from the engine, got %v", err)
	}
	if _, err := hocdb.New("T", round4Dir, round4BarSchema, hocdb.Options{Calendar: nyse, CalendarName: "lse"}); err == nil {
		t.Error("Calendar and CalendarName disagree: expected an error")
	}

	w, err := hocdb.New("T", round4Dir, round4BarSchema, hocdb.Options{CalendarName: "nyse", TimestampUnitNs: 1000})
	if err != nil {
		t.Fatalf("open with calendar: %v", err)
	}
	defer w.Close()
	if w.Calendar() != nyse || w.CalendarName() != "nyse" || w.TimestampUnit() != 1000 {
		t.Errorf("handle calendar %d %q unit %d", w.Calendar(), w.CalendarName(), w.TimestampUnit())
	}
	if !near(w.PeriodsPerYear(60*usec), 252*390, 1e-9) {
		t.Errorf("PeriodsPerYear(1 minute) = %v", w.PeriodsPerYear(60*usec))
	}

	// Thu 2025-09-04, Fri 2025-09-05, Tue 2025-09-09 (Monday missing), one-minute bars
	days := []int64{hocdb.DaysFromCivil(2025, 9, 4), hocdb.DaysFromCivil(2025, 9, 5), hocdb.DaysFromCivil(2025, 9, 9)}
	sessions := make([]*hocdb.Session, 3)
	p := 100.0
	var friOpen float64
	type hlc struct{ h, l, c float64 }
	dayHLC := []hlc{{-1, 1e18, 0}, {-1, 1e18, 0}, {-1, 1e18, 0}}
	for k, d := range days {
		ss, err := hocdb.CalendarSessionForDay(nyse, d)
		if err != nil || ss == nil {
			t.Fatalf("session for day %d: %v, %v", d, ss, err)
		}
		sessions[k] = ss
		for ts := ss.Open; ts < ss.Close; ts += 60 {
			o := p
			p *= 1.0 + 0.001*math.Sin(float64(ts%977))
			hi, lo := math.Max(o, p)*1.001, math.Min(o, p)*0.999
			if k == 1 && ts == ss.Open {
				friOpen = o
			}
			if hi > dayHLC[k].h {
				dayHLC[k].h = hi
			}
			if lo < dayHLC[k].l {
				dayHLC[k].l = lo
			}
			dayHLC[k].c = p
			rec, err := hocdb.CreateRecordBytes(round4BarSchema, ts*usec, o, hi, lo, p, 500+float64(ts%97))
			if err != nil {
				t.Fatalf("record: %v", err)
			}
			if err := w.Append(rec); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	specs := []hocdb.IndicatorSpec{{Kind: "session_range"}, {Kind: "pivots"}} // Param 0 = calendar sessions
	fs := sessions[1]
	res, err := w.Indicators(specs, (fs.Open+100*60)*usec, fs.Close*usec, &hocdb.IndicatorOptions{Lookback: hocdb.Lookback(0)})
	if err != nil {
		t.Fatalf("calendar session kinds: %v", err)
	}
	if res.NRows != 290 || len(res.Names) != 9 {
		t.Fatalf("session kinds over Friday: %d rows, %d outputs (%v)", res.NRows, len(res.Names), res.Names)
	}
	so := res.Columns["session_range_open"]
	if !near(so[0], friOpen, 1e-12) || !near(so[289], friOpen, 1e-12) {
		t.Errorf("session_range open = %v .. %v, expected Friday's first open %v", so[0], so[289], friOpen)
	}
	thuPP := (dayHLC[0].h + dayHLC[0].l + dayHLC[0].c) / 3
	if pp := res.Columns["pivots_pp"][0]; !near(pp, thuPP, 1e-9) {
		t.Errorf("Friday pivot = %v, expected Thursday's (H+L+C)/3 = %v", pp, thuPP)
	}
	// Tuesday: the previous trading day with data is Friday (Monday has no rows)
	tue := sessions[2]
	res, err = w.Indicators(specs[1:], tue.Open*usec, tue.Close*usec, &hocdb.IndicatorOptions{Lookback: hocdb.Lookback(0)})
	if err != nil {
		t.Fatalf("pivots on Tuesday: %v", err)
	}
	friPP := (dayHLC[1].h + dayHLC[1].l + dayHLC[1].c) / 3
	if res.NRows != 390 || !near(res.Columns["pivots_pp"][0], friPP, 1e-9) || !near(res.Columns["pivots_pp"][389], friPP, 1e-9) {
		t.Errorf("Tuesday pivot: %d rows, pp %v .. %v, expected Friday's (H+L+C)/3 = %v", res.NRows, res.Columns["pivots_pp"][0], res.Columns["pivots_pp"][389], friPP)
	}

	h, err := w.Health(0, math.MaxInt64, "close", "volume", 5*60*usec, 0.2)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if len(h) != 19 {
		t.Errorf("health has %d fields, expected 19", len(h))
	}
	if h["count"] != 3*390 || h["n_session_breaks"] != 2 || h["n_missing_sessions"] != 1 || h["n_gaps"] != 1 || h["max_gap"] != float64((60+390*60)*usec) || !(h["closed_span"] > 0) {
		t.Errorf("trading-time health: %v", h)
	}
	sm0, err := w.Summary(0, math.MaxInt64, "close", 0)
	if err != nil {
		t.Fatalf("summary (auto ppy): %v", err)
	}
	sm1, err := w.Summary(0, math.MaxInt64, "close", 252*390)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if !(sm0["ann_vol"] > 0) || !near(sm0["ann_vol"], sm1["ann_vol"], 1e-12) {
		t.Errorf("summary ann_vol with periods_per_year 0 = %v, with 252*390 = %v", sm0["ann_vol"], sm1["ann_vol"])
	}
	if snap, err := w.Snapshot(&hocdb.SnapshotOptions{Bars: 200}); err != nil || snap.Bars != 200 {
		t.Errorf("snapshot with auto periods_per_year: %v, %v", snap, err)
	}

	// a handle without a calendar: session kinds with Param 0 -> CalendarRequired
	plain, err := hocdb.New("P", round4Dir, round4BarSchema, hocdb.Options{})
	if err != nil {
		t.Fatalf("open plain: %v", err)
	}
	rec, _ := hocdb.CreateRecordBytes(round4BarSchema, int64(1), 1.0, 1.0, 1.0, 1.0, 1.0)
	if err := plain.Append(rec); err != nil {
		t.Fatalf("append plain: %v", err)
	}
	plain.Flush()
	if plain.Calendar() != hocdb.CalendarNone || plain.CalendarName() != "" || plain.PeriodsPerYear(60) != 0 {
		t.Errorf("plain handle: calendar %d %q ppy %v", plain.Calendar(), plain.CalendarName(), plain.PeriodsPerYear(60))
	}
	if _, err := plain.IndicatorsTail(1, specs[:1], nil); !errors.Is(err, hocdb.ErrCalendarRequired) {
		t.Errorf("session kind without a calendar: expected ErrCalendarRequired, got %v", err)
	}
	if err := plain.SetCalendar(999); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("SetCalendar(999): expected ErrUnknownCalendar, got %v", err)
	}
	if err := plain.SetCalendarName("nope"); !errors.Is(err, hocdb.ErrUnknownCalendar) {
		t.Errorf("SetCalendarName(nope): expected ErrUnknownCalendar, got %v", err)
	}
	if err := plain.SetCalendarName("fx"); err != nil || plain.Calendar() != hocdb.CalendarFX {
		t.Errorf("SetCalendarName(fx): %v, calendar %d", err, plain.Calendar())
	}
	if err := plain.SetCalendar(hocdb.CalendarCrypto); err != nil || plain.Calendar() != hocdb.CalendarCrypto || plain.CalendarName() != "crypto" {
		t.Errorf("SetCalendar(crypto): %v, calendar %d %q", err, plain.Calendar(), plain.CalendarName())
	}
	if err := plain.SetTimestampUnit(1_000_000_000); err != nil || plain.TimestampUnit() != 1_000_000_000 {
		t.Errorf("SetTimestampUnit: %v, unit %d", err, plain.TimestampUnit())
	}
	if !near(plain.PeriodsPerYear(86400), 365, 1e-9) {
		t.Errorf("PeriodsPerYear after set (daily, seconds) = %v", plain.PeriodsPerYear(86400))
	}
	if _, err := plain.IndicatorsTail(1, specs[:1], nil); err != nil {
		t.Errorf("session kind after SetCalendar + SetTimestampUnit: %v", err)
	}
	plain.Close()
	w.Close()

	// persistence: reopen without config, and a reader
	w3, err := hocdb.New("T", round4Dir, round4BarSchema, hocdb.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w3.Close()
	if w3.Calendar() != nyse || w3.TimestampUnit() != 1000 {
		t.Errorf("reopened handle: calendar %d unit %d", w3.Calendar(), w3.TimestampUnit())
	}
	r, err := hocdb.OpenReader("T", round4Dir, round4BarSchema)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer r.Close()
	if r.Calendar() != nyse || r.CalendarName() != "nyse" || !near(r.PeriodsPerYear(60*usec), 252*390, 1e-9) {
		t.Errorf("reader: calendar %d %q ppy %v", r.Calendar(), r.CalendarName(), r.PeriodsPerYear(60*usec))
	}
	p2, err := hocdb.New("P", round4Dir, round4BarSchema, hocdb.Options{})
	if err != nil {
		t.Fatalf("reopen P: %v", err)
	}
	defer p2.Close()
	if p2.Calendar() != hocdb.CalendarCrypto || p2.TimestampUnit() != 1_000_000_000 {
		t.Errorf("SetCalendar / SetTimestampUnit not persisted: calendar %d unit %d", p2.Calendar(), p2.TimestampUnit())
	}
}

// --- C. backtester -------------------------------------------------------------

var round4TickSchema = []hocdb.Field{
	{Name: "timestamp", Type: hocdb.TypeI64},
	{Name: "price", Type: hocdb.TypeF64},
	{Name: "size", Type: hocdb.TypeF64},
}

func TestRound4Backtest(t *testing.T) {
	// tied example (the same numbers are asserted in the C, Zig and Python references)
	ts := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	open := []float64{100, 101, 102, 98, 95, 97, 99, 103, 104, 102, 100, 99}
	high := []float64{101, 103, 103, 99, 97, 99, 104, 105, 105, 103, 101, 100}
	low := []float64{99, 100, 96, 93, 94, 96, 98, 102, 101, 99, 98, 97}
	close := []float64{100.5, 102.5, 97, 94, 96.5, 98.5, 103.5, 104.5, 102, 100, 98.5, 99.5}
	target := []float64{1, 1, 1, 1, 1, 2, 2, 2, -1, -1, -1, -1}

	p := hocdb.DefaultBacktestParams()
	if p.InitialEquity != 1 || !p.AllowShort || p.FillMode != hocdb.FillNextOpen || p.PositionMode != hocdb.PositionUnits || p.CostBps != 0 {
		t.Errorf("DefaultBacktestParams = %+v", p)
	}
	p.InitialEquity = 1000
	p.CostBps = 10
	p.SlippageBps = 5
	p.StopLoss = 0.05
	p.PeriodsPerYear = 252
	rep, err := hocdb.BacktestArrays(ts, open, high, low, close, target, &p, &hocdb.BacktestOptions{Equity: true, Position: true, MaxTrades: 8})
	if err != nil {
		t.Fatalf("BacktestArrays: %v", err)
	}
	r := rep.Result
	if r.NBars != 12 || r.NTrades != 3 || r.NLongTrades != 2 || r.NShortTrades != 1 {
		t.Errorf("tied counts: %+v", r)
	}
	if !near(r.FinalEquity, 1002.4465295364876, 1e-9) || !near(r.MaxDrawdown, 0.006637024271452185, 1e-12) || r.MaxDrawdownBars != 4 {
		t.Errorf("tied equity / drawdown: final %v dd %v bars %d", r.FinalEquity, r.MaxDrawdown, r.MaxDrawdownBars)
	}
	if !near(r.Sharpe, 0.961805823133203, 1e-9) || !near(r.Turnover, 0.7010422825639875, 1e-9) || !near(r.TotalCost, 0.7009464760125, 1e-9) {
		t.Errorf("tied stats: sharpe %v turnover %v cost %v", r.Sharpe, r.Turnover, r.TotalCost)
	}
	if r.NStopExits != 1 || len(rep.Trades) != 3 {
		t.Fatalf("tied trades: stop exits %d, %d trades", r.NStopExits, len(rep.Trades))
	}
	if tr := rep.Trades[0]; tr.ExitReason != hocdb.ExitStopLoss || !near(tr.ExitPrice, 95.9499760125, 1e-9) || tr.EntryTs != 2 || tr.ExitTs != 4 || tr.Direction != 1 {
		t.Errorf("trades[0] = %+v", tr)
	}
	if tr := rep.Trades[2]; tr.Direction != -1 || tr.ExitTs != 0 || tr.ExitReason != hocdb.ExitEndOfData || hocdb.ExitReasonName(tr.ExitReason) != "end_of_data" {
		t.Errorf("trades[2] = %+v", tr)
	}
	if len(rep.Equity) != 12 || !near(rep.Equity[1], 1001.3484495, 1e-9) || rep.Position[6] != 2 || rep.Position[9] != -1 {
		t.Errorf("per-bar outputs: equity %v position %v", rep.Equity, rep.Position)
	}
	if rep.Cash != nil || rep.Pnl != nil || rep.Drawdown != nil {
		t.Error("unrequested outputs should be nil")
	}
	if len(r.Fields) != 32 || r.Fields["n_trades"] != 3 || r.Fields["final_equity"] != r.FinalEquity || r.Fields["net_pnl"] != r.NetPnl {
		t.Errorf("result fields (%d): %v", len(r.Fields), r.Fields)
	}
	if !near(r.NetPnl, r.FinalEquity-1000, 1e-9) || !near(r.GrossPnl, r.NetPnl+r.TotalCost+r.TotalSlippage, 1e-9) {
		t.Errorf("pnl identities: net %v gross %v cost %v slippage %v", r.NetPnl, r.GrossPnl, r.TotalCost, r.TotalSlippage)
	}
	rep2, err := hocdb.BacktestArrays(ts, open, high, low, close, target, &p, nil)
	if err != nil || rep2.Result.NTrades != 3 || rep2.Trades != nil || rep2.Equity != nil {
		t.Errorf("no outputs / no trade buffer: %v, %+v", err, rep2)
	}
	if rep3, err := hocdb.BacktestArrays(ts, open, high, low, close, target, &p, &hocdb.BacktestOptions{MaxTrades: 1, Cash: true, Pnl: true, Drawdown: true}); err != nil || len(rep3.Trades) != 1 || rep3.Result.NTrades != 3 || len(rep3.Cash) != 12 || len(rep3.Pnl) != 12 || len(rep3.Drawdown) != 12 {
		t.Errorf("MaxTrades 1: %v, %+v", err, rep3)
	}
	bad := p
	bad.PositionMode = "9"
	if _, err := hocdb.BacktestArrays(ts, open, high, low, close, target, &bad, nil); err == nil {
		t.Error("bad position mode: expected an error")
	}
	bad = p
	bad.FillMode = "yesterday"
	if _, err := hocdb.BacktestArrays(ts, open, high, low, close, target, &bad, nil); err == nil {
		t.Error("bad fill mode: expected an error")
	}
	if _, err := hocdb.BacktestArrays(ts, open, high, low, close, target[:11], &p, nil); err == nil || !strings.Contains(err.Error(), "length") {
		t.Errorf("short target: expected a length error, got %v", err)
	}
	if _, err := hocdb.BacktestArrays(ts, open[:3], high, low, close, target, &p, nil); err == nil {
		t.Error("short open: expected a length error")
	}
	if _, err := hocdb.BacktestArrays(nil, nil, nil, nil, nil, nil, &p, nil); err == nil {
		t.Error("empty series: expected an error")
	}
	// mode names and numbers are interchangeable
	fr := p
	fr.PositionMode = hocdb.PositionFraction
	fr.FillMode = hocdb.FillSameClose
	num := p
	num.PositionMode = "1"
	num.FillMode = "1"
	ra, errA := hocdb.BacktestArrays(ts, open, high, low, close, target, &fr, nil)
	rb, errB := hocdb.BacktestArrays(ts, open, high, low, close, target, &num, nil)
	if errA != nil || errB != nil || ra.Result.FinalEquity != rb.Result.FinalEquity || ra.Result.FinalEquity == r.FinalEquity {
		t.Errorf("mode by name vs number: %v %v %v %v", errA, errB, ra.Result.FinalEquity, rb.Result.FinalEquity)
	}

	// walk-forward
	splits := hocdb.WalkForwardSplits(100, 4, 0.5, true)
	if len(splits) != 4 || splits[0] != (hocdb.Split{TrainStart: 0, TrainEnd: 50, TestStart: 50, TestEnd: 62}) || splits[3].TestEnd != 100 {
		t.Errorf("WalkForwardSplits = %+v", splits)
	}
	if rolling := hocdb.WalkForwardSplits(100, 4, 0.5, false); len(rolling) != 4 || rolling[3].TrainStart == 0 || rolling[3].TestEnd != 100 {
		t.Errorf("rolling splits = %+v", rolling)
	}
	if hocdb.WalkForwardSplits(0, 4, 0.5, true) != nil {
		t.Error("WalkForwardSplits(0, ...) should be nil")
	}
	ts100 := make([]int64, 100)
	c100 := make([]float64, 100)
	t100 := make([]float64, 100)
	for i := range ts100 {
		ts100[i] = int64(i + 1)
		c100[i] = 100 + 0.25*float64(i)
		t100[i] = 1
	}
	sp := hocdb.DefaultBacktestParams()
	sp.FillMode = hocdb.FillSameClose
	sp.PositionMode = hocdb.PositionFraction
	rs, err := hocdb.BacktestSplits(ts100, nil, nil, nil, c100, t100, splits, &sp)
	if err != nil || len(rs) != 4 {
		t.Fatalf("BacktestSplits: %v, %d results", err, len(rs))
	}
	if rs[0].NBars != 12 || !near(rs[0].TotalReturn, c100[61]/c100[50]-1, 1e-12) {
		t.Errorf("split 0: %+v", rs[0])
	}
	for k, s := range splits {
		if want := c100[s.TestEnd-1]/c100[s.TestStart] - 1; !near(rs[k].TotalReturn, want, 1e-12) || rs[k].NBars != s.TestEnd-s.TestStart {
			t.Errorf("split %d: total_return %v, expected %v; bars %d", k, rs[k].TotalReturn, want, rs[k].NBars)
		}
	}
	if _, err := hocdb.BacktestSplits(ts100, nil, nil, nil, c100, t100, []hocdb.Split{{TestStart: 90, TestEnd: 120}}, &sp); err == nil {
		t.Error("split beyond the data: expected an error")
	}
	if empty, err := hocdb.BacktestSplits(ts100, nil, nil, nil, c100, t100, nil, &sp); err != nil || len(empty) != 0 {
		t.Errorf("no splits: %v, %v", empty, err)
	}

	// database windows: 20000 ticks every 7 s, crypto calendar, seconds
	round4Setup(t)
	db, err := hocdb.New("B", round4Dir, round4TickSchema, hocdb.Options{Calendar: hocdb.CalendarCrypto, TimestampUnitNs: 1_000_000_000})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	price := 100.0
	for i := 0; i < 20000; i++ {
		price *= 1.0 + 0.0005*math.Sin(float64(i)*0.37) + 0.0002*math.Cos(float64(i)*0.11)
		rec, _ := hocdb.CreateRecordBytes(round4TickSchema, 1700000000+int64(i)*7, price, 1.0+float64(i%5))
		if err := db.Append(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	start := int64(1700000100)
	end := start + 24*3600 // bucket-aligned (300 s bars)
	bars, err := db.OHLCV(start, end, 300, "price", "size")
	if err != nil || len(bars.Close) != 288 {
		t.Fatalf("OHLCV: %v, %d bars", err, len(bars.Close))
	}
	tgt := make([]float64, len(bars.Close))
	for i := range tgt {
		switch {
		case i < 5:
			tgt[i] = 0
		case bars.Close[i] > bars.Close[i-5]:
			tgt[i] = 1
		default:
			tgt[i] = -1
		}
	}
	dp := hocdb.DefaultBacktestParams()
	dp.InitialEquity = 10000
	dp.CostBps = 5
	dp.PositionMode = hocdb.PositionFraction
	dr, err := db.Backtest(tgt, start, end, 300, &dp, nil)
	if err != nil {
		t.Fatalf("db.Backtest: %v", err)
	}
	kp := dp
	kp.PeriodsPerYear = 365 * 288 // the handle derives it from its crypto calendar
	kr, err := hocdb.BacktestArrays(bars.Timestamps, bars.Open, bars.High, bars.Low, bars.Close, tgt, &kp, nil)
	if err != nil {
		t.Fatalf("BacktestArrays on the bars: %v", err)
	}
	if dr.Result.NBars != kr.Result.NBars || dr.Result.FinalEquity != kr.Result.FinalEquity || dr.Result.Sharpe != kr.Result.Sharpe || dr.Result.NTrades != kr.Result.NTrades || !(dr.Result.AnnVol > 0) {
		t.Errorf("db window != bars + calendar ppy:\n db %+v\n arrays %+v", dr.Result, kr.Result)
	}
	if !near(db.PeriodsPerYear(300), 365*288, 1e-9) {
		t.Errorf("db.PeriodsPerYear(300) = %v", db.PeriodsPerYear(300))
	}
	if _, err := db.Backtest(tgt[:len(tgt)-1], start, end, 300, &dp, nil); err == nil || !strings.Contains(err.Error(), "length") {
		t.Errorf("length mismatch: expected an error mentioning the length, got %v", err)
	}
	if _, err := db.Backtest(nil, start, end, 300, &dp, nil); err == nil {
		t.Error("empty target: expected an error")
	}
	if _, err := db.Backtest(tgt, start, end, 300, &dp, &hocdb.BacktestOptions{Columns: &hocdb.IndicatorColumns{Close: "nope"}}); err == nil {
		t.Error("unknown close column: expected an error")
	}
	tail, err := db.BacktestTail(tgt[len(tgt)-50:], 300, &dp, &hocdb.BacktestOptions{Equity: true, MaxTrades: 100})
	if err != nil || tail.Result.NBars != 50 || len(tail.Equity) != 50 {
		t.Fatalf("BacktestTail: %v, %+v", err, tail)
	}
	all, err := db.OHLCV(math.MinInt64, math.MaxInt64, 300, "price", "size")
	if err != nil || len(all.Close) < 50 {
		t.Fatalf("all bars: %v", err)
	}
	m := len(all.Close)
	tr, err := hocdb.BacktestArrays(all.Timestamps[m-50:], all.Open[m-50:], all.High[m-50:], all.Low[m-50:], all.Close[m-50:], tgt[len(tgt)-50:], &kp, &hocdb.BacktestOptions{Equity: true})
	if err != nil || tr.Result.FinalEquity != tail.Result.FinalEquity || tr.Equity[49] != tail.Equity[49] {
		t.Errorf("tail != last 50 bars: %v, %v vs %v", err, tr.Result.FinalEquity, tail.Result.FinalEquity)
	}
	// nil params = the engine defaults
	if d, err := db.Backtest(tgt, start, end, 300, nil, nil); err != nil || d.Result.NBars != 288 || d.Result.NTrades == 0 {
		t.Errorf("Backtest with nil params: %v, %+v", err, d)
	}
}

// --- D. universe -----------------------------------------------------------------

func TestRound4Universe(t *testing.T) {
	p := hocdb.DefaultUniverseParams()
	if p.MomShort != 5 || p.MomMid != 20 || p.MomLong != 60 || p.CorrPeriod != 60 || p.WeightsMode != hocdb.WeightsEqual {
		t.Errorf("DefaultUniverseParams = %+v", p)
	}
	const N, M = 100, 3
	closes := make([][]float64, M)
	vols := make([][]float64, M)
	ts := make([]int64, N)
	for k := range closes {
		closes[k] = make([]float64, N)
		vols[k] = make([]float64, N)
	}
	for i := 0; i < N; i++ {
		fi := float64(i)
		ts[i] = 1000 + int64(i)*60
		closes[0][i] = 100 + 5*math.Sin(fi*0.2) + 0.1*fi
		closes[1][i] = 50 + 3*math.Cos(fi*0.15) - 0.05*fi
		closes[2][i] = closes[0][i]
		vols[0][i] = 1000 + float64(i%7)*10
		vols[1][i] = 2000
		vols[2][i] = 500
	}
	p.MomLong = 30
	p.CorrPeriod = 30
	p.BetaPeriod = 30
	p.SmaPeriod = 20
	rep, err := hocdb.UniverseArrays(closes, vols, ts, &p)
	if err != nil {
		t.Fatalf("UniverseArrays: %v", err)
	}
	s := rep.Summary
	if s.NTickers != 3 || s.NBars != N || s.FirstTs != 1000 || s.LastTs != 1000+99*60 {
		t.Errorf("summary basics: %+v", s)
	}
	if len(rep.Rows) != 3 || len(rep.Corr) != 3 || len(rep.Corr[0]) != 3 {
		t.Fatalf("shape: %d rows, %d corr rows", len(rep.Rows), len(rep.Corr))
	}
	if !near(rep.Corr[0][2], 1, 1e-12) || !near(rep.Corr[2][0], 1, 1e-12) || rep.Corr[0][0] != 1 || rep.Corr[1][1] != 1 || !near(rep.Corr[0][1], rep.Corr[1][0], 1e-15) {
		t.Errorf("corr matrix: %v", rep.Corr)
	}
	if rep.Rows[0].MaxCorrIndex != 2 || rep.Rows[2].MaxCorrIndex != 0 || !near(rep.Rows[0].MaxCorr, 1, 1e-12) {
		t.Errorf("max_corr partner: %+v / %+v", rep.Rows[0], rep.Rows[2])
	}
	if rep.Rows[0].RankMomMid != rep.Rows[2].RankMomMid || math.IsNaN(rep.Rows[1].Beta) || math.IsNaN(rep.Rows[1].Vol) || rep.Rows[0].LastClose != closes[0][N-1] {
		t.Errorf("ranks / features: %+v", rep.Rows)
	}
	if !(s.BreadthUp >= 0 && s.BreadthUp <= 1) || !(s.AvgPairCorr <= 1) || math.IsNaN(s.Dispersion) {
		t.Errorf("summary ranges: %+v", s)
	}
	if len(rep.Rows[0].Fields) != 21 || len(s.Fields) != 16 || rep.Rows[0].Fields["max_corr_index"] != 2 || s.Fields["n_bars"] != N || s.Fields["last_ts"] != float64(s.LastTs) {
		t.Errorf("introspection: %d row fields, %d summary fields", len(rep.Rows[0].Fields), len(s.Fields))
	}
	if !math.IsNaN(rep.Rows[0].VolumeRatio) && rep.Rows[0].VolumeRatio <= 0 {
		t.Errorf("volume_ratio with volumes = %v", rep.Rows[0].VolumeRatio)
	}
	noVol, err := hocdb.UniverseArrays(closes, nil, nil, &p)
	if err != nil || !math.IsNaN(noVol.Rows[0].VolumeRatio) || noVol.Summary.FirstTs != 0 {
		t.Errorf("no volumes / no ts: %v, %+v", err, noVol)
	}
	if _, err := hocdb.UniverseArrays([][]float64{closes[0], closes[1][:50]}, nil, nil, &p); err == nil {
		t.Error("ragged closes: expected an error")
	}
	if _, err := hocdb.UniverseArrays(closes, vols[:2], nil, &p); err == nil {
		t.Error("fewer volume series: expected an error")
	}
	if _, err := hocdb.UniverseArrays(closes, nil, ts[:10], &p); err == nil {
		t.Error("short ts: expected an error")
	}
	if _, err := hocdb.UniverseArrays(nil, nil, nil, &p); err == nil {
		t.Error("no closes: expected an error")
	}
	bad := p
	bad.WeightsMode = "cap"
	if _, err := hocdb.UniverseArrays(closes, vols, ts, &bad); err == nil {
		t.Error("bad weights mode: expected an error")
	}
	vw := p
	vw.WeightsMode = hocdb.WeightsVolume
	if vrep, err := hocdb.UniverseArrays(closes, vols, ts, &vw); err != nil || vrep.Summary.NBars != N {
		t.Errorf("volume-weighted: %v", err)
	}

	// databases: 3 handles, the third with a missing bar every 7th
	round4Setup(t)
	dbs := make([]*hocdb.DB, M)
	for k := range dbs {
		db, err := hocdb.New([]string{"U0", "U1", "U2"}[k], round4Dir, round4TickSchema, hocdb.Options{})
		if err != nil {
			t.Fatalf("open U%d: %v", k, err)
		}
		defer db.Close()
		dbs[k] = db
		for i := 0; i < N; i++ {
			if k == 2 && i%7 == 6 {
				continue
			}
			rec, _ := hocdb.CreateRecordBytes(round4TickSchema, ts[i], closes[k][i], vols[k][i])
			if err := db.Append(rec); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		db.Flush()
	}
	drep, err := hocdb.Universe(dbs, nil, 100, 0, &p)
	if err != nil {
		t.Fatalf("Universe: %v", err)
	}
	if drep.Summary.NTickers != 3 || drep.Summary.NBars != N-N/7 || drep.Summary.LastTs != ts[N-1] {
		t.Errorf("joined bars: %+v", drep.Summary)
	}
	// hand-join: the same rows through the arrays entry point
	var jts []int64
	jc := make([][]float64, M)
	jv := make([][]float64, M)
	for i := 0; i < N; i++ {
		if i%7 == 6 {
			continue
		}
		jts = append(jts, ts[i])
		for k := 0; k < M; k++ {
			jc[k] = append(jc[k], closes[k][i])
			jv[k] = append(jv[k], vols[k][i])
		}
	}
	jrep, err := hocdb.UniverseArrays(jc, jv, jts, &p)
	if err != nil || len(jts) != drep.Summary.NBars {
		t.Fatalf("hand join: %v, %d bars vs %d", err, len(jts), drep.Summary.NBars)
	}
	for k := 0; k < M; k++ {
		d, j := drep.Rows[k], jrep.Rows[k]
		if !sameFloat(d.MomMid, j.MomMid) || !sameFloat(d.Beta, j.Beta) || d.RankMomLong != j.RankMomLong || !sameFloat(d.Vol, j.Vol) {
			t.Errorf("row %d: db %+v\n   arrays %+v", k, d, j)
		}
		for l := 0; l < M; l++ {
			if !sameFloat(drep.Corr[k][l], jrep.Corr[k][l]) {
				t.Errorf("corr[%d][%d]: db %v arrays %v", k, l, drep.Corr[k][l], jrep.Corr[k][l])
			}
		}
	}
	if drep.Summary.AvgPairCorr != jrep.Summary.AvgPairCorr || drep.Summary.BreadthSma != jrep.Summary.BreadthSma {
		t.Errorf("summary: db %+v\n arrays %+v", drep.Summary, jrep.Summary)
	}
	if brep, err := hocdb.Universe(dbs, nil, 0, 60, &p); err != nil || brep.Summary.NBars < 31 {
		t.Errorf("bucket mode, default n_bars: %v, %+v", err, brep)
	}
	if erep, err := hocdb.Universe(dbs, &hocdb.IndicatorColumns{Close: "price", Volume: "size"}, 100, 0, nil); err != nil || erep.Summary.NBars != N-N/7 {
		t.Errorf("explicit columns, default params: %v", err)
	}
	if _, err := hocdb.Universe(dbs, &hocdb.IndicatorColumns{Volume: "size"}, 0, 0, &p); err == nil {
		t.Error("no close column: expected an error")
	}
	if _, err := hocdb.Universe(dbs, &hocdb.IndicatorColumns{Close: "nope"}, 0, 0, &p); err == nil {
		t.Error("unknown close column: expected an error")
	}
	if _, err := hocdb.Universe(nil, nil, 0, 0, &p); err == nil {
		t.Error("no databases: expected an error")
	}
	if _, err := hocdb.Universe([]*hocdb.DB{dbs[0], nil}, nil, 0, 0, &p); err == nil {
		t.Error("nil database: expected an error")
	}
}
