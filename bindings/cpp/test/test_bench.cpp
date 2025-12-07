#include "hocdb_cpp.h"
#include <iostream>
#include <chrono>
#include <vector>

int main() {
    std::cout << "Testing HOCDB C++ bindings...\n" << std::endl;
    
    // Clean up any previous test data
    system("rm -rf test_data_cpp");
    
    try {
        // Initialize the database
        std::cout << "1. Initializing database..." << std::endl;
        hocdb::Database db("TEST_CPP", "test_data_cpp");
        
        // Test 1: Append performance
        std::cout << "\n2. Testing append performance..." << std::endl;
        
        auto start = std::chrono::high_resolution_clock::now();
        const int num_records = 1000000;  // 1M records
        for (int i = 0; i < num_records; i++) {
            int64_t timestamp = 1600000000 + i;
            double usd = 50000.0 + (i % 1000) * 0.01;
            double volume = 1.0 + (i % 100) * 0.01;
            
            db.append(timestamp, usd, volume);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto append_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        double append_time = append_duration.count() / 1000.0;
        double append_ops = num_records / append_time;
        
        std::cout << "Appended " << num_records << " records in " << append_time << " seconds" << std::endl;
        std::cout << "Append performance: " << static_cast<long long>(append_ops) << " ops/sec" << std::endl;
        
        // Flush to ensure data is written
        db.flush();
        
        // Test 2: Load performance (Zero-Copy with RAII)
        std::cout << "\n3. Testing zero-copy load with RAII wrapper..." << std::endl;
        
        start = std::chrono::high_resolution_clock::now();
        
        // Use RAII wrapper for automatic memory management
        auto buffer = hocdb::load_with_raii(db);
        
        end = std::chrono::high_resolution_clock::now();
        auto load_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        double load_time = load_duration.count() / 1000000000.0;  // Convert to seconds
        
        std::cout << "Loaded " << buffer.size() << " records in " << load_time << " seconds" << std::endl;
        std::cout << "Load performance: instantaneous (zero-copy)" << std::endl;
        
        // Verify data was loaded correctly
        if (buffer.size() > 0) {
            std::cout << "First record: ts=" << buffer[0].timestamp 
                      << ", usd=" << buffer[0].usd 
                      << ", vol=" << buffer[0].volume << std::endl;
            if (buffer.size() > 1) {
                std::cout << "Last record: ts=" << buffer[buffer.size()-1].timestamp 
                          << ", usd=" << buffer[buffer.size()-1].usd 
                          << ", vol=" << buffer[buffer.size()-1].volume << std::endl;
            }
        }
        
        // Test 3: Manual memory management approach
        std::cout << "\n4. Testing manual memory management approach..." << std::endl;
        
        auto [data, length] = db.load();
        std::cout << "Manually loaded " << length << " records" << std::endl;
        
        // Access some data to prove it's valid
        if (length > 0) {
            std::cout << "Sample from manual load - ts=" << data[0].timestamp 
                      << ", usd=" << data[0].usd 
                      << ", vol=" << data[0].volume << std::endl;
        }
        
        // Free manually
        db.free_data(const_cast<hocdb::TradeData*>(data));
        std::cout << "Manual memory freed" << std::endl;
        
        // Test 4: Small data verification
        std::cout << "\n5. Testing small dataset for accuracy..." << std::endl;
        
        // Create a small database for verification
        hocdb::Database small_db("SMALL_CPP", "test_data_cpp_small");
        
        // Add a few known records
        small_db.append(100, 1.1, 10.1);
        small_db.append(200, 2.2, 20.2);
        small_db.append(300, 3.3, 30.3);
        small_db.flush();
        
        // Load and verify with RAII
        auto small_buffer = hocdb::load_with_raii(small_db);
        if (small_buffer.size() != 3) {
            std::cerr << "Expected 3 records, got " << small_buffer.size() << std::endl;
            return 1;
        }
        
        if (small_buffer[0].timestamp != 100 || small_buffer[0].usd != 1.1 || small_buffer[0].volume != 10.1) {
            std::cerr << "Data verification failed for first record" << std::endl;
            return 1;
        }
        
        if (small_buffer[2].timestamp != 300 || small_buffer[2].usd != 3.3 || small_buffer[2].volume != 30.3) {
            std::cerr << "Data verification failed for last record" << std::endl;
            return 1;
        }
        
        std::cout << "Small dataset verification passed!" << std::endl;
        
        std::cout << "\nC++ bindings test completed successfully!" << std::endl;
        return 0;
        
    } catch (const hocdb::Exception& e) {
        std::cerr << "HOCDB Error: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Standard Error: " << e.what() << std::endl;
        return 1;
    }
}