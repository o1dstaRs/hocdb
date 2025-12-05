#ifndef HOCDB_CPP_H
#define HOCDB_CPP_H

#include "hocdb.h"
#include <stdexcept>
#include <vector>
#include <memory>
#include <string>
#include <cstring>

namespace hocdb {

/**
 * @brief Exception class for HOCDB errors
 */
class Exception : public std::runtime_error {
public:
    explicit Exception(const std::string& message) : std::runtime_error(message) {}
};

/**
 * @brief Field definition for schema
 */
struct Field {
    std::string name;
    int type; // 1=i64, 2=f64, 3=u64
};

/**
 * @brief C++ wrapper class for the HOCDB database
 */
class Database {
private:
    HOCDBHandle handle_;
    size_t record_size_;

public:
    /**
     * @brief Initialize the database with dynamic schema
     * @param ticker Ticker symbol
     * @param path Directory path for data
     * @param schema Vector of Field definitions
     * @param max_file_size Maximum file size (0 for default)
     * @param overwrite_on_full Whether to overwrite when full
     * @param flush_on_write Whether to flush on every write
     * @throws Exception if initialization fails
     */
    Database(const std::string& ticker, const std::string& path, const std::vector<Field>& schema, int64_t max_file_size = 0, bool overwrite_on_full = true, bool flush_on_write = false) {
        std::vector<CField> c_schema;
        c_schema.reserve(schema.size());
        
        record_size_ = 0;
        for (const auto& field : schema) {
            c_schema.push_back({field.name.c_str(), field.type});
            switch (field.type) {
                case HOCDB_TYPE_I64: record_size_ += 8; break;
                case HOCDB_TYPE_F64: record_size_ += 8; break;
                case HOCDB_TYPE_U64: record_size_ += 8; break;
                default: throw Exception("Unsupported field type");
            }
        }

        handle_ = hocdb_init(ticker.c_str(), path.c_str(), c_schema.data(), c_schema.size(), max_file_size, overwrite_on_full ? 1 : 0, flush_on_write ? 1 : 0);
        if (!handle_) {
            throw Exception("Failed to initialize HOCDB");
        }
    }

    /**
     * @brief Destructor - closes the database
     */
    ~Database() {
        if (handle_) {
            hocdb_close(handle_);
        }
    }

    /**
     * @brief Move constructor
     */
    Database(Database&& other) noexcept : handle_(other.handle_), record_size_(other.record_size_) {
        other.handle_ = nullptr;
    }

    /**
     * @brief Move assignment operator
     */
    Database& operator=(Database&& other) noexcept {
        if (this != &other) {
            if (handle_) {
                hocdb_close(handle_);
            }
            handle_ = other.handle_;
            record_size_ = other.record_size_;
            other.handle_ = nullptr;
        }
        return *this;
    }

    /**
     * @brief Copy constructor is deleted
     */
    Database(const Database&) = delete;

    /**
     * @brief Copy assignment operator is deleted
     */
    Database& operator=(const Database&) = delete;

    /**
     * @brief Append a raw record to the database
     * @param data Pointer to data
     * @param len Length of data
     * @throws Exception if append fails or length mismatch
     */
    void append(const void* data, size_t len) {
        if (len != record_size_) {
            throw Exception("Data length mismatch with schema record size");
        }
        if (hocdb_append(handle_, data, len) != 0) {
            throw Exception("Failed to append record to HOCDB");
        }
    }

    /**
     * @brief Append a struct to the database (template)
     * @param record The struct to append
     * @throws Exception if append fails or size mismatch
     */
    template<typename T>
    void append(const T& record) {
        append(&record, sizeof(T));
    }

    /**
     * @brief Flush the database (force write to disk)
     * @throws Exception if flush fails
     */
    void flush() {
        if (hocdb_flush(handle_) != 0) {
            throw Exception("Failed to flush HOCDB");
        }
    }

    /**
     * @brief Load all records into memory with zero-copy
     * @return std::pair containing pointer to raw bytes and total length in bytes
     * 
     * IMPORTANT: The returned pointer is valid only until the next operation on the database
     *            or until the database is closed. The caller is responsible for calling
     *            free_data() to free the memory.
     */
    std::vector<uint8_t> load() {
        size_t len = 0;
        void* data = hocdb_load(handle_, &len);
        if (!data && len > 0) {
             throw Exception("Failed to load data from HOCDB");
        }
        if (!data && len == 0) {
            return {};
        }
        
        std::vector<uint8_t> result(static_cast<uint8_t*>(data), static_cast<uint8_t*>(data) + len);
        hocdb_free(data);
        return result;
    }

    /**
     * @brief Query records in a time range with optional filters
     * @param start_ts Start timestamp
     * @param end_ts End timestamp
     * @param filters Vector of HOCDBFilter structs to apply
     * @return std::vector<uint8_t> containing the raw bytes of the matching records
     */
    std::vector<uint8_t> query(int64_t start_ts, int64_t end_ts, const std::vector<HOCDBFilter>& filters = {}) {
        size_t out_len = 0;
        const HOCDBFilter* filters_ptr = filters.empty() ? nullptr : filters.data();
        void* data = hocdb_query(handle_, start_ts, end_ts, filters_ptr, filters.size(), &out_len);
        if (!data) {
            return {}; // Return empty vector on failure or empty result
        }
        
        // Copy data to vector
        std::vector<uint8_t> result(static_cast<uint8_t*>(data), static_cast<uint8_t*>(data) + out_len);
        
        // Free C memory
        hocdb_free(data);
        
        return result;
    }

    /**
     * @brief Get statistics for a specific field within a time range.
     * @param start_ts Start timestamp
     * @param end_ts End timestamp
     * @param field_index Index of the field to get statistics for
     * @return HOCDBStats struct containing min, max, sum, count, and avg
     * @throws std::runtime_error if getting stats fails
     */
    HOCDBStats getStats(int64_t start_ts, int64_t end_ts, size_t field_index) {
        HOCDBStats stats;
        if (hocdb_get_stats(handle_, start_ts, end_ts, field_index, &stats) != 0) {
            throw std::runtime_error("getStats failed");
        }
        return stats;
    }

    /**
     * @brief Get the latest value and timestamp for a specific field.
     * @param field_index Index of the field to get the latest value for
     * @return std::pair containing the latest value (double) and its timestamp (int64_t)
     * @throws std::runtime_error if getting the latest value fails
     */
    std::pair<double, int64_t> getLatest(size_t field_index) {
        double val;
        int64_t ts;
        if (hocdb_get_latest(handle_, field_index, &val, &ts) != 0) {
            throw std::runtime_error("getLatest failed");
        }
        return {val, ts};
    }

    /**
     * @brief Free memory allocated by load()
     * @param ptr Pointer returned by load()
     */
    void free_data(void* ptr) {
        hocdb_free(ptr);
    }

    /**
     * @brief Closes the database handle (explicit close)
     */
    void close() {
        if (handle_) {
            hocdb_close(handle_);
            handle_ = nullptr;
        }
    }

    /**
     * @brief Check if the database handle is valid
     */
    bool is_valid() const {
        return handle_ != nullptr;
    }

    /**
     * @brief Get the underlying handle (for advanced usage)
     */
    HOCDBHandle get_handle() const {
        return handle_;
    }
    
    size_t get_record_size() const {
        return record_size_;
    }
};


} // namespace hocdb

#endif // HOCDB_CPP_H