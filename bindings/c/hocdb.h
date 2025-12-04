#ifndef HOCDB_H
#define HOCDB_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Field type constants
#define HOCDB_TYPE_I64 1
#define HOCDB_TYPE_F64 2
#define HOCDB_TYPE_U64 3

// Structure for schema field definition
typedef struct {
    const char* name;
    int type;
} CField;

// Database handle
typedef void* HOCDBHandle;

/**
 * Initialize the database with dynamic schema and config
 * @param ticker Ticker symbol as null-terminated string
 * @param path Directory path for data as null-terminated string
 * @param schema Array of CField structs defining the schema
 * @param schema_len Number of fields in the schema
 * @param max_file_size Maximum file size (0 for default)
 * @param overwrite_on_full Whether to overwrite when full (1 for true, 0 for false)
 * @param flush_on_write Whether to flush on every write (1 for true, 0 for false)
 * @return Database handle or NULL on failure
 */
HOCDBHandle hocdb_init(const char* ticker, const char* path, const CField* schema, size_t schema_len, int64_t max_file_size, int overwrite_on_full, int flush_on_write);

/**
 * Append a raw record to the database
 * @param handle Database handle
 * @param data Pointer to raw data bytes
 * @param len Length of data in bytes
 * @return 0 on success, non-zero on failure
 */
int hocdb_append(HOCDBHandle handle, const void* data, size_t len);

/**
 * Flush the database (force write to disk)
 * @param handle Database handle
 * @return 0 on success, non-zero on failure
 */
int hocdb_flush(HOCDBHandle handle);

/**
 * Load all records into memory with zero-copy
 * @param handle Database handle
 * @param out_len Output parameter to store the number of bytes loaded
 * @return Pointer to raw data bytes (allocated with c_allocator, caller must free with hocdb_free)
 *         Returns NULL on failure
 * 
 * IMPORTANT: The returned pointer is valid only until the next operation on the database
 *            or until the database is closed. The caller is responsible for calling
 *            hocdb_free() to free the memory.
 */
void* hocdb_load(HOCDBHandle handle, size_t* out_len);

/**
 * Query records in a time range
 * @param handle Database handle
 * @param start_ts Start timestamp (inclusive)
 * @param end_ts End timestamp (exclusive)
 * @param out_len Output parameter to store the number of bytes loaded
 * @return Pointer to raw data bytes (allocated with c_allocator, caller must free with hocdb_free)
 *         Returns NULL on failure
 */
void* hocdb_query(HOCDBHandle handle, int64_t start_ts, int64_t end_ts, size_t* out_len);

/**
 * Free memory allocated by hocdb_load
 * @param ptr Pointer returned by hocdb_load
 */
void hocdb_free(void* ptr);

/**
 * Close and free the database handle
 * @param handle Database handle to close
 */
void hocdb_close(HOCDBHandle handle);

#ifdef __cplusplus
}
#endif

#endif // HOCDB_H