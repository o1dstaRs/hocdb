#include "hocdb_cpp.h"
#include <iostream>
#include <cassert>
#include <vector>
#include <filesystem>

struct TradeData {
    int64_t timestamp;
    double value;
};

int main() {
    try {
        std::string test_dir = "b_cpp_test_data/agg";
        if (std::filesystem::exists(test_dir)) {
            std::filesystem::remove_all(test_dir);
        }
        std::filesystem::create_directories(test_dir);

        std::vector<hocdb::Field> schema = {
            {"timestamp", HOCDB_TYPE_I64},
            {"value", HOCDB_TYPE_F64}
        };

        hocdb::Database db("TEST_CPP_AGG", test_dir, schema);

        std::cout << "Appending data..." << std::endl;
        db.append(TradeData{100, 10.0});
        db.append(TradeData{200, 20.0});
        db.append(TradeData{300, 30.0});
        db.flush();

        std::cout << "Testing getLatest..." << std::endl;
        auto [val, ts] = db.getLatest(1); // value index = 1
        std::cout << "Latest: value=" << val << ", timestamp=" << ts << std::endl;
        
        assert(val == 30.0);
        assert(ts == 300);

        std::cout << "Testing getStats..." << std::endl;
        auto stats = db.getStats(0, 400, 1);
        std::cout << "Stats: min=" << stats.min << ", max=" << stats.max << ", sum=" << stats.sum 
                  << ", count=" << stats.count << ", mean=" << stats.mean << std::endl;

        assert(stats.count == 3);
        assert(stats.min == 10.0);
        assert(stats.max == 30.0);
        assert(stats.sum == 60.0);
        assert(stats.mean == 20.0);

        std::cout << "C++ Aggregation Test Passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
}
