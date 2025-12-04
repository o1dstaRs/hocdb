#include "hocdb_cpp.h"
#include <iostream>
#include <chrono>
#include <vector>

// Define the structure that matches our schema
struct TradeData {
    int64_t timestamp;
    double usd;
    double volume;
};

int main() {
    try {
        // Initialize the database
        std::cout << "Initializing HOCDB..." << std::endl;
        
        // Define Schema
        std::vector<hocdb::Field> schema = {
            {"timestamp", HOCDB_TYPE_I64},
            {"usd", HOCDB_TYPE_F64},
            {"volume", HOCDB_TYPE_F64}
        };

        // Create database instance with default config
        hocdb::Database db("EXAMPLE", "example_data", schema);
        
        // Add some sample data
        std::cout << "Adding sample records..." << std::endl;
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        
        for (int i = 0; i < 5; ++i) {
            TradeData record;
            record.timestamp = now + i * 1000;  // Add 1 second intervals
            record.usd = 100.0 + i * 10.0;
            record.volume = 1000.0 + i * 100.0;
            
            db.append(record);
            std::cout << "Added: ts=" << record.timestamp << ", usd=" << record.usd << ", vol=" << record.volume << std::endl;
        }
        
        // Flush to ensure data is written
        db.flush();
        std::cout << "Data flushed to disk." << std::endl;
        
        // Load data with zero-copy
        std::cout << "\nLoading data with zero-copy..." << std::endl;
        auto [data_ptr, byte_len] = db.load();
        
        size_t count = byte_len / sizeof(TradeData);
        const TradeData* data = static_cast<const TradeData*>(data_ptr);

        std::cout << "Loaded " << count << " records:" << std::endl;
        for (size_t i = 0; i < count; ++i) {
            std::cout << "  Record " << i << ": ts=" << data[i].timestamp 
                      << ", usd=" << data[i].usd 
                      << ", vol=" << data[i].volume << std::endl;
        }
        
        // Free the loaded data
        db.free_data(data_ptr);
        std::cout << "\nData freed." << std::endl;
        
        // Example using RAII wrapper for automatic memory management
        std::cout << "\nLoading data with RAII wrapper..." << std::endl;
        {
            auto buffer = hocdb::load_with_raii<TradeData>(db);
            std::cout << "Buffer contains " << buffer.size() << " records:" << std::endl;
            for (size_t i = 0; i < buffer.size(); ++i) {
                std::cout << "  Record " << i << ": ts=" << buffer[i].timestamp 
                          << ", usd=" << buffer[i].usd 
                          << ", vol=" << buffer[i].volume << std::endl;
            }
            // buffer automatically frees memory when it goes out of scope
        }
        std::cout << "RAII buffer automatically freed." << std::endl;
        
        std::cout << "\nHOCDB C++ example completed successfully!" << std::endl;
        
    } catch (const hocdb::Exception& e) {
        std::cerr << "HOCDB Error: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Standard Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}