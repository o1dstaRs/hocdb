// C++ cross-binding consistency program (see README.md).
#include "hocdb_cpp.h"
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

// Tiny helpers reusing the same flat-JSON assumptions as consistency_c.c.
static std::string slurp(const char *path) {
  std::ifstream f(path);
  std::stringstream ss;
  ss << f.rdbuf();
  return ss.str();
}
static size_t find_key(const std::string &j, const std::string &key, size_t from = 0) {
  size_t p = j.find("\"" + key + "\"", from);
  if (p == std::string::npos) return p;
  p = j.find(':', p);
  return p == std::string::npos ? p : p + 1;
}
static long long get_int(const std::string &j, const std::string &key, size_t from = 0) {
  size_t p = find_key(j, key, from);
  return p == std::string::npos ? 0 : std::stoll(j.substr(p));
}
static double get_num(const std::string &j, const std::string &key, size_t from = 0) {
  size_t p = find_key(j, key, from);
  return p == std::string::npos ? 0 : std::stod(j.substr(p));
}
static std::string get_str(const std::string &j, const std::string &key, size_t from = 0) {
  size_t p = find_key(j, key, from);
  if (p == std::string::npos) return "";
  p = j.find('"', p) + 1;
  return j.substr(p, j.find('"', p) - p);
}
static void emit(double x) {
  if (std::isnan(x)) std::printf("null");
  else if (std::isinf(x)) std::printf(x > 0 ? "\"inf\"" : "\"-inf\"");
  else std::printf("%.17g", x);
}

int main(int argc, char **argv) {
  if (argc < 2) return 2;
  std::string j = slurp(argv[1]);
  std::vector<hocdb::Field> schema = {{"timestamp", HOCDB_TYPE_I64}, {"price", HOCDB_TYPE_F64}, {"size", HOCDB_TYPE_F64},
                                      {"bid", HOCDB_TYPE_F64},       {"ask", HOCDB_TYPE_F64},   {"side", HOCDB_TYPE_BOOL}};
  hocdb::Database db(get_str(j, "ticker"), get_str(j, "dir"), schema);
  hocdb::IndicatorOptions opts;
  hocdb::IndicatorColumns cols;
  cols.close = "price";
  cols.volume = "size";
  opts.columns = cols;
  opts.bucket = get_int(j, "bucket");
  std::vector<hocdb::IndicatorSpec> specs;
  size_t p = j.find('[', find_key(j, "specs"));
  size_t arr_end = j.find(']', p);
  while (true) {
    size_t obj = j.find('{', p);
    if (obj == std::string::npos || obj > arr_end) break;
    size_t end = j.find('}', obj);
    std::string t = j.substr(obj, end - obj + 1);
    hocdb::IndicatorSpec s;
    s.kind = get_str(t, "kind");
    s.period = (uint32_t)get_int(t, "period");
    s.period2 = (uint32_t)get_int(t, "period2");
    s.period3 = (uint32_t)get_int(t, "period3");
    s.period4 = (uint32_t)get_int(t, "period4");
    s.param = get_num(t, "param");
    s.param2 = get_num(t, "param2");
    specs.push_back(s);
    p = end + 1;
  }
  auto res = db.indicatorsTail((size_t)get_int(j, "tail"), specs, opts);
  std::printf("{\"binding\":\"cpp\",\"timestamps\":[");
  for (size_t i = 0; i < res.timestamps.size(); i++) std::printf("%s%lld", i ? "," : "", (long long)res.timestamps[i]);
  std::printf("],\"columns\":{");
  for (size_t k = 0; k < res.names.size(); k++) {
    std::printf("%s\"%s\":[", k ? "," : "", res.names[k].c_str());
    for (size_t i = 0; i < res.n_rows; i++) { if (i) std::printf(","); emit(res.outputs[k][i]); }
    std::printf("]");
  }
  std::printf("}");
  size_t sn = find_key(j, "snapshot");
  auto snap = db.snapshotMap(&cols, (size_t)get_int(j, "bars", sn), get_int(j, "bucket", sn), get_num(j, "periods_per_year", sn));
  std::printf(",\"snapshot\":{");
  bool first = true;
  for (auto &kv : snap) {
    std::printf("%s\"%s\":", first ? "" : ",", kv.first.c_str());
    first = false;
    if (kv.first == "timestamp" || kv.first == "bars") std::printf("%lld", (long long)kv.second);
    else emit(kv.second);
  }
  std::printf("}");
  size_t su = find_key(j, "summary");
  auto sum = db.summaryMap(get_int(j, "start", su), get_int(j, "end", su), get_str(j, "field", su), get_num(j, "periods_per_year", su));
  std::printf(",\"summary\":{");
  first = true;
  for (auto &kv : sum) {
    std::printf("%s\"%s\":", first ? "" : ",", kv.first.c_str());
    first = false;
    if (kv.first == "count") std::printf("%lld", (long long)kv.second);
    else emit(kv.second);
  }
  std::printf("}");
  size_t oh = find_key(j, "ohlcv");
  auto bars = db.ohlcv(get_int(j, "start", oh), get_int(j, "end", oh), get_int(j, "bucket", oh), "price", "size");
  std::printf(",\"ohlcv\":{\"timestamps\":[");
  for (size_t i = 0; i < bars.timestamps.size(); i++) std::printf("%s%lld", i ? "," : "", (long long)bars.timestamps[i]);
  const char *names[] = {"open", "high", "low", "close", "volume", "count"};
  const std::vector<double> *arrs[] = {&bars.open, &bars.high, &bars.low, &bars.close, &bars.volume, &bars.count};
  for (int a = 0; a < 6; a++) {
    std::printf("],\"%s\":[", names[a]);
    for (size_t i = 0; i < arrs[a]->size(); i++) { if (i) std::printf(","); emit((*arrs[a])[i]); }
  }
  std::printf("]}}\n");
  return 0;
}
