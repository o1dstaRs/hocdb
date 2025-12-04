# Agent Guidelines

## Testing Folder Convention

When creating test data directories, use the following naming convention:
`b_<lang>_test_data`

Examples:
- `b_node_test_data` for Node.js tests
- `b_bun_test_data` for Bun tests
- `b_cpp_test_data` for C++ tests

This ensures that test artifacts are easily identifiable and can be ignored by version control if necessary.

## Project Structure

- **`src/`**: Core Zig implementation.
    - `root.zig`: Main library entry point (`DynamicTimeSeriesDB`).
    - `bindings.zig`: Exported C-ABI functions and Node.js/Bun bindings.
    - `c_bindings.zig`: Dedicated C-ABI exports for C/C++.
    - `bench.zig`: Benchmarking tool.
- **`bindings/`**: Language-specific bindings.
    - `node/`: Node.js bindings (N-API).
    - `bun/`: Bun bindings (FFI).
    - `c/`: C headers and example.
    - `cpp/`: C++ headers (wrapper around C) and tests.
    - `python/`: Python bindings.
- **`.github/workflows/`**: CI/CD configuration.

## Testing

To verify changes, run the following commands:

### Core Zig
```bash
zig build test
zig build bench
```

### Node.js
```bash
zig build bindings
node bindings/node/test.js
```

### Bun
```bash
bun run bindings/bun/test.ts
```

### C/C++
```bash
zig build c-bindings
mkdir -p test_binaries

# Compile and run C++ tests
zig c++ -o test_binaries/cpp_test bindings/cpp/test.cpp -I zig-out/include -L zig-out/lib -lhocdb_c -std=c++17
./test_binaries/cpp_test

# Compile and run C tests
gcc -o test_binaries/c_simple_test bindings/c/simple_test.c -I zig-out/include -L zig-out/lib -lhocdb_c
./test_binaries/c_simple_test
```

### Python
```bash
zig build python-bindings
python3 bindings/python/test.py
```
