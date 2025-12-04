# C/C++ Bindings - Build Instructions

## Building the C/C++ Bindings

The HOCDB C/C++ bindings are built as part of the main project:

```bash
# Build the C bindings (shared library and headers)
zig build c-bindings
```

This will create:
- Shared library: `zig-out/lib/libhocdb_c.dylib` (macOS) or `libhocdb_c.so` (Linux) or `libhocdb_c.dll` (Windows)
- Headers: `zig-out/include/hocdb.h` and `zig-out/include/hocdb_cpp.h`

## Compiling C Example

After building the bindings, compile the C example:

```bash
# On macOS:
gcc -Izig-out/include -Lzig-out/lib -lhocdb_c -o c_example bindings/c/example.c

# On Linux:
gcc -Izig-out/include -Lzig-out/lib -Wl,-rpath,./zig-out/lib -lhocdb_c -o c_example bindings/c/example.c

# Run the example:
./c_example
```

## Compiling C++ Example

```bash
# On macOS:
g++ -std=c++11 -Izig-out/include -Lzig-out/lib -lhocdb_c -o cpp_example bindings/cpp/example.cpp

# On Linux:
g++ -std=c++11 -Izig-out/include -Lzig-out/lib -Wl,-rpath,./zig-out/lib -lhocdb_c -o cpp_example bindings/cpp/example.cpp

# Run the example:
./cpp_example
```

## Running C++ Tests

```bash
# On macOS:
g++ -std=c++11 -Izig-out/include -Lzig-out/lib -lhocdb_c -o cpp_test bindings/cpp/test.cpp
./cpp_test

# On Linux:
g++ -std=c++11 -Izig-out/include -Lzig-out/lib -Wl,-rpath,./zig-out/lib -lhocdb_c -o cpp_test bindings/cpp/test.cpp
./cpp_test
```

## Zero-Copy Memory Management

Important: When using `hocdb_load()`, the returned pointer is allocated by Zig's C allocator and must be freed with `hocdb_free()` to prevent memory leaks. The C++ RAII wrapper (`hocdb::DataBuffer`) handles this automatically.

## API Reference

### C API

- `HOCDBHandle hocdb_init(const char* ticker, const char* path)` - Initialize database with default config
- `HOCDBHandle hocdb_init_config(const char* ticker, const char* path, int64_t max_file_size, int overwrite_on_full)` - Initialize with custom config
- `int hocdb_append(HOCDBHandle handle, int64_t timestamp, double usd, double volume)` - Append a record
- `int hocdb_flush(HOCDBHandle handle)` - Flush to disk
- `const TradeData* hocdb_load(HOCDBHandle handle, size_t* out_len)` - Load all records (zero-copy)
- `void hocdb_free(void* ptr)` - Free loaded data
- `void hocdb_close(HOCDBHandle handle)` - Close database

### Data Structures

```c
typedef struct {
    int64_t timestamp;
    double usd;
    double volume;
} TradeData;

typedef struct HOCDB_t* HOCDBHandle;
```