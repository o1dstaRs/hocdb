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
    std::pair<void*, size_t> load() {
        size_t len = 0;
        void* data = hocdb_load(handle_, &len);
        if (!data && len > 0) { // If len is 0, data might be null or valid pointer to 0 bytes?
             // hocdb_load returns null on error. If empty, it might return non-null pointer to 0 bytes or null?
             // Zig allocator.alloc(0) returns a slice with ptr...
             // Let's assume if it returns null it failed.
             throw Exception("Failed to load data from HOCDB");
        }
        if (!data && len == 0) {
            // Empty DB
            return std::make_pair(nullptr, 0);
        }
        return std::make_pair(data, len);
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

/**
 * @brief RAII wrapper for zero-copy loaded data
 * This ensures that the memory allocated by load() is properly freed
 */
template<typename T>
class DataBuffer {
private:
    T* data_;
    size_t count_;
    Database* db_ref_;

public:
    /**
     * @brief Constructor taking the data pointer and database reference
     */
    DataBuffer(void* data, size_t byte_len, Database& db) 
        : data_(static_cast<T*>(data)), count_(byte_len / sizeof(T)), db_ref_(&db) {
        if (byte_len % sizeof(T) != 0) {
            // Warning: byte length not multiple of struct size
        }
    }

    /**
     * @brief Destructor - automatically frees the data
     */
    ~DataBuffer() {
        if (data_) {
            db_ref_->free_data(data_);
        }
    }

    /**
     * @brief Move constructor
     */
    DataBuffer(DataBuffer&& other) noexcept 
        : data_(other.data_), count_(other.count_), db_ref_(other.db_ref_) {
        other.data_ = nullptr;
        other.count_ = 0;
    }

    /**
     * @brief Move assignment operator
     */
    DataBuffer& operator=(DataBuffer&& other) noexcept {
        if (this != &other) {
            if (data_) {
                db_ref_->free_data(data_);
            }
            data_ = other.data_;
            count_ = other.count_;
            db_ref_ = other.db_ref_;
            other.data_ = nullptr;
            other.count_ = 0;
        }
        return *this;
    }

    /**
     * @brief Copy constructor is deleted
     */
    DataBuffer(const DataBuffer&) = delete;

    /**
     * @brief Copy assignment operator is deleted
     */
    DataBuffer& operator=(const DataBuffer&) = delete;

    /**
     * @brief Get pointer to the data
     */
    const T* data() const { return data_; }

    /**
     * @brief Get number of records in the buffer
     */
    size_t size() const { return count_; }

    /**
     * @brief Check if the buffer is empty
     */
    bool empty() const { return count_ == 0; }

    /**
     * @brief Access operator for direct access to data
     */
    const T& operator[](size_t index) const {
        if (index >= count_) {
            throw Exception("Index out of bounds");
        }
        return data_[index];
    }

    /**
     * @brief Get iterator to beginning of data
     */
    const T* begin() const { return data_; }

    /**
     * @brief Get iterator to end of data
     */
    const T* end() const { return data_ + count_; }
};

/**
 * @brief Convenience function to load data with automatic memory management
 * @param db Database instance to load from
 * @return DataBuffer RAII wrapper around the loaded data
 */
template<typename T>
inline DataBuffer<T> load_with_raii(Database& db) {
    auto [data, len] = db.load();
    return DataBuffer<T>(data, len, db);
}

} // namespace hocdb

#endif // HOCDB_CPP_H