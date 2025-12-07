#include <iostream>
#include <vector>
#include <cassert>
#include <filesystem>
#include "../hocdb_cpp.h"

using namespace hocdb;

int main() {
    const std::string ticker = "TEST_CPP_VERIFY";
    const std::string data_dir = "b_cpp_verify_data";

    // Cleanup
    if (std::filesystem::exists(data_dir)) {
        std::filesystem::remove_all(data_dir);
    }

    // Define Schema
    std::vector<Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"value", HOCDB_TYPE_F64},
        {"category", HOCDB_TYPE_I64}
    };

    struct Record {
        int64_t timestamp;
        double value;
        int64_t category;
    };

    try {
        std::cout << "Initializing DB..." << std::endl;
        Database db(ticker, data_dir, schema);

        std::cout << "Appending data..." << std::endl;
        // Append records
        Record r1 = {100, 1.0, 1};
        db.append(r1);
        Record r2 = {200, 2.0, 2};
        db.append(r2);
        Record r3 = {300, 3.0, 1};
        db.append(r3);

        db.flush();

        std::cout << "Querying with filter (category=1)..." << std::endl;
        std::map<std::string, Database::FilterValue> filters;
        filters["category"] = int64_t(1);

        auto data = db.query(0, 1000, filters);
        
        size_t record_size = 8 + 8 + 8;
        size_t count = data.size() / record_size;
        std::cout << "Filtered result count: " << count << std::endl;

        if (count != 2) {
            std::cerr << "Expected 2 records, got " << count << std::endl;
            return 1;
        }

        std::cout << "✅ C++ Verification Test Passed!" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    // Cleanup
    if (std::filesystem::exists(data_dir)) {
        std::filesystem::remove_all(data_dir);
    }

    return 0;
}
