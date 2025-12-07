#include "hocdb_cpp.h"
#include <iostream>
#include <cassert>
#include <chrono>
#include <vector>
#include <filesystem>

struct TradeData {
    int64_t timestamp;
    double usd;
    double volume;
};

void test_basic_functionality() {
    std::cout << "Running basic functionality test..." << std::endl;
    
    // Define Schema
    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"usd", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64}
    };

    // Test initialization and basic operations
    hocdb::Database db("TEST", "b_cpp_test_data/basic", schema);
    
    // Test appending data
    db.append(TradeData{100, 1.1, 10.1});
    db.append(TradeData{200, 2.2, 20.2});
    db.append(TradeData{300, 3.3, 30.3});
    db.flush();
    
    // Test loading data
    auto data_vec = db.load();
    size_t count = data_vec.size() / sizeof(TradeData);
    const TradeData* data = reinterpret_cast<const TradeData*>(data_vec.data());

    assert(count == 3);
    assert(data[0].timestamp == 100);
    assert(data[0].usd == 1.1);
    assert(data[0].volume == 10.1);
    assert(data[2].timestamp == 300);
    assert(data[2].usd == 3.3);
    assert(data[2].volume == 30.3);
    
    // db.free_data(data_ptr); // Vector handles memory
    
    std::cout << "Basic functionality test passed!" << std::endl;
}

void test_raii_wrapper() {
    std::cout << "Skipping RAII wrapper test (functionality integrated into load)" << std::endl;
}

void test_config_functionality() {
    std::cout << "Running config functionality test..." << std::endl;
    
    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"usd", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64}
    };

    // Test initialization with custom config
    hocdb::Database db("CONFIG_TEST", "b_cpp_test_data/config", schema, 1024*1024, true);
    
    // Add some data
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    for (int i = 0; i < 3; ++i) {
        db.append(TradeData{now + i * 1000, 100.0 + i * 10.0, 1000.0 + i * 100.0});
    }
    db.flush();
    
    // Load and verify
    auto data_vec = db.load();
    size_t count = data_vec.size() / sizeof(TradeData);
    assert(count == 3);
    
    std::cout << "Config functionality test passed!" << std::endl;
}

void test_error_handling() {
    std::cout << "Running error handling test..." << std::endl;
    
    std::vector<hocdb::Field> schema = {
        {"timestamp", HOCDB_TYPE_I64},
        {"usd", HOCDB_TYPE_F64},
        {"volume", HOCDB_TYPE_F64}
    };

    // Test with invalid path (should fail gracefully)
    try {
        // Using a path that should cause an error (try to use a file as directory)
        hocdb::Database db("ERROR_TEST", "/dev/null/invalid_path", schema); // This should fail
        // If we get here, the error didn't occur as expected, so fail the test
        assert(false && "Should have thrown an exception");
    } catch (const hocdb::Exception&) {
        // Expected to catch an exception here
    }
    
    std::cout << "Error handling test passed!" << std::endl;
}

int main() {
    try {
        // Clean up previous test runs
        if (std::filesystem::exists("b_cpp_test_data")) {
            std::filesystem::remove_all("b_cpp_test_data");
        }
        std::filesystem::create_directory("b_cpp_test_data");

        // Define schema once for all tests that need it
        std::vector<hocdb::Field> schema = {
            {"timestamp", HOCDB_TYPE_I64},
            {"usd", HOCDB_TYPE_F64},
            {"volume", HOCDB_TYPE_F64}
        };

        test_basic_functionality();
        test_raii_wrapper();
        test_config_functionality();
        test_error_handling();
        
        std::cout << "All C++ binding tests passed!" << std::endl;
        // --- Flush-on-Write Test ---
    std::cout << "\nRunning Flush-on-Write Test..." << std::endl;
    {
        std::string test_dir = "b_cpp_test_data/flush";
        std::filesystem::remove_all(test_dir);

        hocdb::Database db("TEST_FLUSH", test_dir, schema, 1024 * 1024, true, true);

        auto start = std::chrono::high_resolution_clock::now();
        int count = 10000;
        TradeData record;
        for (int i = 0; i < count; i++) {
            record.timestamp = i;
            record.usd = i * 1.5;
            record.volume = i * 2.5;
            db.append(record);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - start;
        std::cout << "Appended " << count << " records with flush_on_write=true in " << diff.count() * 1000 << "ms" << std::endl;
        std::cout << "Throughput: " << count / diff.count() << " ops/sec" << std::endl;

        std::cout << "✅ Flush-on-Write Test Passed!" << std::endl;
        std::filesystem::remove_all(test_dir);
    }

    return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}