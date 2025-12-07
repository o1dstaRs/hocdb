#!/bin/bash
set -e

echo "========================================"
echo "HOCDB Bindings Verification Script"
echo "========================================"

# 1. Build C Bindings
echo ""
echo "[1/5] Building C Bindings (Zig)..."
zig build c-bindings
echo "✅ C Bindings built"

# 2. Run Bun Tests
echo ""
echo "[2/5] Running Bun Tests..."
bun run bindings/bun/test.ts
echo "✅ Bun Tests passed"

# 3. Run Python Tests
echo ""
echo "[3/5] Running Python Tests..."
python3 bindings/python/test_query.py
echo "✅ Python Tests passed"

# 4. Run Go Tests
echo ""
echo "[4/5] Running Go Tests..."
export DYLD_LIBRARY_PATH=$(pwd)/zig-out/lib:$DYLD_LIBRARY_PATH
export LD_LIBRARY_PATH=$(pwd)/zig-out/lib:$LD_LIBRARY_PATH
go test -v bindings/go/hocdb_test.go bindings/go/hocdb.go
echo "✅ Go Tests passed"

# 5. Run C++ Tests
echo ""
echo "[5/5] Running C++ Tests..."
mkdir -p test_binaries
clang++ -std=c++17 bindings/c/test_cpp.cpp -o test_binaries/test_cpp_verify -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_cpp_verify
clang++ -std=c++17 bindings/c/test_filter_syntax.cpp -o test_binaries/test_cpp_filter -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_cpp_filter
echo "✅ C++ Tests passed"

# 6. Run C Recovery and Filter Tests
echo ""
echo "[6/6] Running C Tests..."
mkdir -p test_binaries
clang -o test_binaries/test_auto_inc_recovery bindings/c/test_auto_inc_recovery.c -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_auto_inc_recovery
clang -o test_binaries/test_c_filter bindings/c/test_filter_syntax.c -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_c_filter
echo "✅ C Tests passed"

# 7. Run Node.js Tests
echo ""
echo "[7/7] Running Node.js Tests..."
zig build bindings
node bindings/node/test.js
echo "✅ Node.js Tests passed"

echo ""
echo "========================================"
echo "🎉 ALL BINDINGS VERIFIED SUCCESSFULLY!"
echo "========================================"
