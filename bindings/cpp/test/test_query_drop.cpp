/**
 * Test for query() and drop() methods in C++ API
 */
#include "hocdb_cpp.h"
#include <iostream>
#include <vector>
#include <map>
#include <cstdlib>
#include <sys/stat.h>
#include <dirent.h>

// Define record structure matching schema
struct __attribute__((packed)) Trade {
    int64_t timestamp;
    double price;
    double volume;
    bool is_buy;
};

static bool file_exists(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

static int dir_file_count(const std::string& dir_path) {
    DIR* dir = opendir(dir_path.c_str());
    if (!dir) return 0;

    int count = 0;
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] != '.') {
            count++;
        }
    }
    closedir(dir);
    return count;
}

static void rm_rf(const std::string& path) {
    std::string cmd = "rm -rf " + path;
    system(cmd.c_str());
}

int test_query() {
    std::cout << "Testing query()...\n";

    const std::string test_dir = "../../../b_cpp_test_query";
    rm_rf(test_dir);

    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64},
        {"is_buy", HOCDB_TYPE_BOOL}
    };

    try {
        hocdb::Database db("QUERY_TEST", test_dir, schema);

        // Append some trades
        db.append(Trade{1620000000, 100.0, 10.0, true});
        db.append(Trade{1620000001, 200.0, 20.0, false});
        db.append(Trade{1620000002, 300.0, 30.0, true});
        db.append(Trade{1620000003, 400.0, 40.0, false});
        db.append(Trade{1620000004, 500.0, 50.0, true});
        db.flush();

        // Test 1: Query without filters
        auto data = db.query(1620000000, 1620000005, std::vector<HOCDBFilter>{});
        size_t count = data.size() / sizeof(Trade);
        if (count != 5) {
            std::cerr << "FAIL: Expected 5 records, got " << count << "\n";
            rm_rf(test_dir);
            return 1;
        }
        std::cout << "  Query without filters: " << count << " records (OK)\n";

        // Test 2: Query with time range
        data = db.query(1620000001, 1620000003, std::vector<HOCDBFilter>{});
        count = data.size() / sizeof(Trade);
        if (count != 2) {
            std::cerr << "FAIL: Range query expected 2 records, got " << count << "\n";
            rm_rf(test_dir);
            return 1;
        }
        std::cout << "  Query with range [1, 3): " << count << " records (OK)\n";

        // Test 3: Query with HOCDBFilter for boolean (is_buy = true)
        std::vector<HOCDBFilter> bool_filters;
        HOCDBFilter bf;
        bf.field_index = 3;
        bf.type = HOCDB_TYPE_BOOL;
        bf.val_bool = true;
        bool_filters.push_back(bf);

        data = db.query(1620000000, 1620000005, bool_filters);
        count = data.size() / sizeof(Trade);
        if (count != 3) {
            std::cerr << "FAIL: Filtered query expected 3 buy orders, got " << count << "\n";
            rm_rf(test_dir);
            return 1;
        }
        std::cout << "  Query with filter (is_buy=true): " << count << " records (OK)\n";

        // Test 4: Query with price filter using HOCDBFilter
        std::vector<HOCDBFilter> price_filters;
        HOCDBFilter f;
        f.field_index = 1;
        f.type = HOCDB_TYPE_F64;
        f.val_f64 = 300.0;
        price_filters.push_back(f);

        data = db.query(1620000000, 1620000005, price_filters);
        count = data.size() / sizeof(Trade);
        if (count != 1) {
            std::cerr << "FAIL: Price filter expected 1 record, got " << count << "\n";
            rm_rf(test_dir);
            return 1;
        }
        std::cout << "  Query with filter (price=300.0): " << count << " records (OK)\n";

        db.close();

    } catch (const hocdb::Exception& e) {
        std::cerr << "FAIL: Exception: " << e.what() << "\n";
        rm_rf(test_dir);
        return 1;
    }

    rm_rf(test_dir);
    std::cout << "PASS: query() tests\n\n";
    return 0;
}

int test_drop() {
    std::cout << "Testing drop()...\n";

    const std::string test_dir = "../../../b_cpp_test_drop";
    rm_rf(test_dir);

    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"price", HOCDB_TYPE_F64}
    };

    try {
        hocdb::Database db("DROP_TEST", test_dir, schema);

        // Append some data
        struct __attribute__((packed)) {
            int64_t timestamp;
            double price;
        } record = {1620000000, 50000.0};

        db.append(record);
        record.timestamp = 1620000001;
        db.append(record);
        db.flush();

        // Verify data directory exists
        if (!file_exists(test_dir)) {
            std::cerr << "FAIL: Data directory should exist after writes\n";
            return 1;
        }

        int files_before = dir_file_count(test_dir);
        if (files_before == 0) {
            std::cerr << "FAIL: Data files should exist before drop\n";
            rm_rf(test_dir);
            return 1;
        }
        std::cout << "  Files before drop: " << files_before << "\n";

        // Drop the database
        db.drop();

        // Verify handle is invalid
        if (db.is_valid()) {
            std::cerr << "FAIL: Handle should be invalid after drop\n";
            rm_rf(test_dir);
            return 1;
        }

        // Verify data files are deleted
        int files_after = dir_file_count(test_dir);
        std::cout << "  Files after drop: " << files_after << "\n";

        if (files_after > 0) {
            std::cerr << "FAIL: Data files should be deleted after drop\n";
            rm_rf(test_dir);
            return 1;
        }

    } catch (const hocdb::Exception& e) {
        std::cerr << "FAIL: Exception: " << e.what() << "\n";
        rm_rf(test_dir);
        return 1;
    }

    rm_rf(test_dir);
    std::cout << "PASS: drop() test\n\n";
    return 0;
}

int main() {
    std::cout << "=== C++ Bindings Query and Drop Tests ===\n\n";

    int result = 0;
    result |= test_query();
    result |= test_drop();

    if (result == 0) {
        std::cout << "All tests PASSED!\n";
    } else {
        std::cout << "Some tests FAILED!\n";
    }

    return result;
}
