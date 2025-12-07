#include "../hocdb_cpp.h"
#include <iostream>
#include <vector>
#include <filesystem>
#include <cassert>

namespace fs = std::filesystem;

int main() {
    const std::string TICKER = "TEST_CPP_FILTER";
    const std::string DATA_DIR = "b_cpp_test_filter_syntax";

    // Cleanup
    if (fs::exists(DATA_DIR)) {
        fs::remove_all(DATA_DIR);
    }

    try {
        std::vector<hocdb::Field> schema = {
            {"timestamp", HOCDB_TYPE_I64},
            {"price", HOCDB_TYPE_F64},
            {"event", HOCDB_TYPE_I64}
        };

        std::cout << "Initializing DB..." << std::endl;
        hocdb::Database db(TICKER, DATA_DIR, schema);

        std::cout << "Appending data..." << std::endl;
        
        struct Record {
            int64_t timestamp;
            double price;
            int64_t event;
        };

        // 1. event = 0
        Record r1 = {100, 1.0, 0};
        db.append(r1);
        // 2. event = 1
        Record r2 = {200, 2.0, 1};
        db.append(r2);
        // 3. event = 2
        Record r3 = {300, 3.0, 2};
        db.append(r3);

        // Query with map filter: { "event": 1 }
        std::cout << "Querying with filter map { event: 1 }..." << std::endl;
        std::map<std::string, hocdb::Database::FilterValue> filters;
        filters["event"] = int64_t(1);

        auto data = db.query(0, 1000, filters);
        
        size_t count = data.size() / sizeof(Record);
        std::cout << "Results count: " << count << std::endl;

        if (count != 1) {
            throw std::runtime_error("Expected 1 result");
        }

        const Record* res = reinterpret_cast<const Record*>(data.data());
        std::cout << "Result: TS=" << res->timestamp << ", Event=" << res->event << std::endl;

        if (res->event != 1) {
            throw std::runtime_error("Expected event 1");
        }

        std::cout << "✅ C++ Filter Syntax Test Passed!" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Test Failed: " << e.what() << std::endl;
        return 1;
    }

    // Cleanup
    if (fs::exists(DATA_DIR)) {
        fs::remove_all(DATA_DIR);
    }

    return 0;
}
