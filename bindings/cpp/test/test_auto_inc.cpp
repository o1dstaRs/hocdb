#include "hocdb_cpp.h"
#include <iostream>
#include <cassert>
#include <vector>
#include <filesystem>

struct TradeData {
    int64_t timestamp;
    double usd;
    double volume;
};

void test_auto_increment() {
    std::cout << "Running C++ Auto-Increment Test..." << std::endl;
    
    std::string test_dir = "b_cpp_test_data/auto_inc";
    if (std::filesystem::exists(test_dir)) {
        std::filesystem::remove_all(test_dir);
    }
    std::filesystem::create_directories(test_dir);

    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"usd", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64}
    };

    // 1. Initialize with auto_increment = true
    {
        // auto_increment is the last argument (7th)
        hocdb::Database db("TEST_AUTO_INC", test_dir, schema, 0, true, false, true);

        for (int i = 0; i < 10; ++i) {
            // Pass 0 as timestamp, should be overwritten
            db.append(TradeData{0, (double)i, (double)i * 10.0});
        }
        db.flush();

        auto data_vec = db.load();
        size_t count = data_vec.size() / sizeof(TradeData);
        const TradeData* data = reinterpret_cast<const TradeData*>(data_vec.data());

        assert(count == 10);
        for (size_t i = 0; i < count; ++i) {
            assert(data[i].timestamp == (int64_t)(i + 1));
            assert(data[i].usd == (double)i);
        }
    }

    // 2. Reopen and append more
    {
        hocdb::Database db("TEST_AUTO_INC", test_dir, schema, 0, true, false, true);

        for (int i = 10; i < 15; ++i) {
            db.append(TradeData{999, (double)i, (double)i * 10.0});
        }
        db.flush();

        auto data_vec = db.load();
        size_t count = data_vec.size() / sizeof(TradeData);
        const TradeData* data = reinterpret_cast<const TradeData*>(data_vec.data());

        assert(count == 15);
        for (size_t i = 0; i < count; ++i) {
            assert(data[i].timestamp == (int64_t)(i + 1));
            assert(data[i].usd == (double)i);
        }
    }

    std::filesystem::remove_all(test_dir);
    std::cout << "C++ Auto-Increment Test Passed!" << std::endl;
}

int main() {
    try {
        test_auto_increment();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
}
