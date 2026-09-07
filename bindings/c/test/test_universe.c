// C ABI test for the universe (cross-sectional) features: arrays entry point,
// several database handles joined on timestamps, introspection.
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../hocdb.h"

static int failures = 0;
#define CHECK(cond, msg) do { if (!(cond)) { printf("  FAIL: %s\n", msg); failures++; } } while (0)

typedef struct { int64_t timestamp; double price; double size; } Tick;

int main(void) {
  HOCDBUniverseParams p;
  hocdb_universe_params_default(&p);
  CHECK(p.mom_short == 5 && p.mom_mid == 20 && p.mom_long == 60 && p.corr_period == 60 && p.weights_mode == 0, "params default");
  CHECK(sizeof(HOCDBUniverseParams) == hocdb_universe_params_size() && sizeof(HOCDBUniverseRow) == hocdb_universe_row_size() && sizeof(HOCDBUniverseSummary) == hocdb_universe_summary_size(), "struct sizes");
  CHECK(hocdb_universe_row_field_count() == 21 && strcmp(hocdb_universe_row_field_name(18), "max_corr_index") == 0 && hocdb_universe_row_field_type(18) == 3 && hocdb_universe_row_field_type(0) == 2, "row introspection");
  CHECK(hocdb_universe_summary_field_count() == 16 && strcmp(hocdb_universe_summary_field_name(15), "last_ts") == 0 && hocdb_universe_summary_field_type(15) == 1, "summary introspection");
  // arrays: 3 tickers x 100 bars, ticker 2 == ticker 0 (perfect correlation)
  enum { N = 100, M = 3 };
  static double c[M][N], v[M][N];
  static int64_t ts[N];
  for (int i = 0; i < N; i++) {
    ts[i] = 1000 + i * 60;
    c[0][i] = 100 + 5 * sin(i * 0.2) + 0.1 * i;
    c[1][i] = 50 + 3 * cos(i * 0.15) - 0.05 * i;
    c[2][i] = c[0][i];
    v[0][i] = 1000 + (i % 7) * 10; v[1][i] = 2000; v[2][i] = 500;
  }
  const double *closes[M] = {c[0], c[1], c[2]};
  const double *vols[M] = {v[0], v[1], v[2]};
  HOCDBUniverseRow rows[M];
  double corr[M * M];
  HOCDBUniverseSummary s;
  p.mom_long = 30; p.corr_period = 30; p.beta_period = 30; p.sma_period = 20;
  CHECK(hocdb_universe_arrays(closes, vols, M, N, ts, &p, rows, corr, &s) == 0, "arrays rc");
  CHECK(s.n_tickers == 3 && s.n_bars == N && s.first_ts == 1000 && s.last_ts == 1000 + 99 * 60, "summary basics");
  CHECK(fabs(corr[2] - 1.0) < 1e-12 && fabs(corr[6] - 1.0) < 1e-12 && corr[0] == 1.0 && fabs(corr[1] - corr[3]) < 1e-15, "corr matrix: identical tickers, symmetry, diagonal");
  CHECK(rows[0].max_corr_index == 2 && rows[2].max_corr_index == 0 && fabs(rows[0].max_corr - 1.0) < 1e-12, "max_corr partner");
  CHECK(rows[0].rank_mom_mid == rows[2].rank_mom_mid && !isnan(rows[1].beta) && !isnan(rows[1].vol) && rows[0].last_close == c[0][N - 1], "ranks / features");
  CHECK(s.breadth_up >= 0 && s.breadth_up <= 1 && s.avg_pair_corr <= 1.0 && !isnan(s.dispersion), "summary ranges");
  CHECK(hocdb_universe_arrays(closes, NULL, M, N, NULL, &p, rows, NULL, &s) == 0 && isnan(rows[0].volume_ratio) && s.first_ts == 0, "no volumes / no ts / no corr");

  // databases: 3 handles, the third with a missing bar every 7th
  const char *dir = "b_c_test_universe";
  system("rm -rf b_c_test_universe");
  CField schema[] = {{"timestamp", HOCDB_TYPE_I64}, {"price", HOCDB_TYPE_F64}, {"size", HOCDB_TYPE_F64}};
  HOCDBConfig cfg; memset(&cfg, 0, sizeof cfg); cfg.auto_migrate = 1;
  const char *names[M] = {"U0", "U1", "U2"};
  HOCDBHandle hs[M];
  for (int k = 0; k < M; k++) {
    hs[k] = hocdb_init_ex(names[k], dir, schema, 3, &cfg);
    CHECK(hs[k] != NULL, "open");
    for (int i = 0; i < N; i++) {
      if (k == 2 && i % 7 == 6) continue;
      Tick t = {ts[i], c[k][i], v[k][i]};
      hocdb_append(hs[k], &t, sizeof t);
    }
    hocdb_flush(hs[k]);
  }
  HOCDBIndicatorColumns cols = {-1, -1, -1, 1, 2, -1, -1, -1};
  HOCDBUniverseRow drows[M];
  double dcorr[M * M];
  HOCDBUniverseSummary ds;
  CHECK(hocdb_universe(hs, M, &cols, 100, 0, &p, drows, dcorr, &ds) == 0, "db universe rc");
  CHECK(ds.n_tickers == 3 && ds.n_bars == N - N / 7 && ds.last_ts == ts[N - 1], "joined bars (every 7th dropped)");
  // hand-join: the same rows through the arrays entry point
  static double jc[M][N], jv[M][N];
  static int64_t jts[N];
  size_t nj = 0;
  for (int i = 0; i < N; i++) {
    if (i % 7 == 6) continue;
    jts[nj] = ts[i];
    for (int k = 0; k < M; k++) { jc[k][nj] = c[k][i]; jv[k][nj] = v[k][i]; }
    nj++;
  }
  const double *jcl[M] = {jc[0], jc[1], jc[2]};
  const double *jvl[M] = {jv[0], jv[1], jv[2]};
  HOCDBUniverseRow jrows[M];
  double jcorr[M * M];
  HOCDBUniverseSummary js;
  CHECK(hocdb_universe_arrays(jcl, jvl, M, nj, jts, &p, jrows, jcorr, &js) == 0 && nj == ds.n_bars, "hand join rc");
  int same = 1;
  for (int k = 0; k < M; k++) {
    if (!(drows[k].mom_mid == jrows[k].mom_mid || (isnan(drows[k].mom_mid) && isnan(jrows[k].mom_mid)))) same = 0;
    if (!(drows[k].beta == jrows[k].beta || (isnan(drows[k].beta) && isnan(jrows[k].beta)))) same = 0;
    if (drows[k].rank_mom_long != jrows[k].rank_mom_long) same = 0;
  }
  for (int i = 0; i < M * M; i++) if (!(dcorr[i] == jcorr[i] || (isnan(dcorr[i]) && isnan(jcorr[i])))) same = 0;
  CHECK(same && ds.avg_pair_corr == js.avg_pair_corr && ds.breadth_sma == js.breadth_sma, "db join == hand join");
  CHECK(hocdb_universe(hs, M, &cols, 0, 60, &p, drows, NULL, &ds) == 0 && ds.n_bars >= 31, "bucket mode, default n_bars");
  HOCDBIndicatorColumns nocl = {-1, -1, -1, -1, -1, -1, -1, -1};
  CHECK(hocdb_universe(hs, M, &nocl, 0, 0, &p, drows, NULL, &ds) == -3, "missing close -> -3");
  for (int k = 0; k < M; k++) hocdb_close(hs[k]);
  system("rm -rf b_c_test_universe");
  if (failures) { printf("C universe API test FAILED (%d)\n", failures); return 1; }
  printf("C universe API test passed\n");
  return 0;
}
