#!/bin/bash
set -e

echo "========================================"
echo "HOCDB Bindings Verification Script"
echo "========================================"

# 1. Build C Bindings
echo ""
echo "[1/8] Building C Bindings (Zig)..."
zig build c-bindings
echo "✅ C Bindings built"

# 2. Run Bun Tests
echo ""
echo "[2/8] Running Bun Tests..."
bun run bindings/bun/test/test.ts
bun run bindings/bun/test/test_async_drop.ts
echo "✅ Bun Tests passed"

# 3. Run Python Tests
echo ""
echo "[3/8] Running Python Tests..."
export PYTHONPATH=$(pwd)/bindings/python:$PYTHONPATH
python3 bindings/python/test/test_query.py
echo "✅ Python Tests passed"

# 4. Run Go Tests
echo ""
echo "[4/8] Running Go Tests..."
export DYLD_LIBRARY_PATH=$(pwd)/zig-out/lib:$DYLD_LIBRARY_PATH
export LD_LIBRARY_PATH=$(pwd)/zig-out/lib:$LD_LIBRARY_PATH
(cd bindings/go && go test -v ./test/...)
echo "✅ Go Tests passed"

# 5. Run C++ ABI Tests (testing C header from C++)
echo ""
echo "[5/8] Running C++ ABI Tests..."
mkdir -p test_binaries
clang++ -std=c++17 bindings/c/test/test_cpp.cpp -o test_binaries/test_cpp_verify -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_cpp_verify
rm test_binaries/test_cpp_verify
clang++ -std=c++17 bindings/c/test/test_filter_syntax.cpp -o test_binaries/test_cpp_filter -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_cpp_filter
rm test_binaries/test_cpp_filter
echo "✅ C++ ABI Tests passed"

# 6. Run C++ Wrapper Tests (testing C++ wrapper)
echo ""
echo "[6/8] Running C++ Wrapper Tests..."
clang++ -std=c++17 bindings/cpp/test/test.cpp -o test_binaries/test_cpp_wrapper -I bindings/c -I bindings/cpp -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_cpp_wrapper
rm test_binaries/test_cpp_wrapper
echo "✅ C++ Wrapper Tests passed"

# 7. Run C Recovery and Filter Tests
echo ""
echo "[7/8] Running C Tests..."
mkdir -p test_binaries
clang -o test_binaries/test_auto_inc_recovery bindings/c/test/test_auto_inc_recovery.c -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_auto_inc_recovery
rm test_binaries/test_auto_inc_recovery
clang -o test_binaries/test_c_filter bindings/c/test/test_filter_syntax.c -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/test_c_filter
rm test_binaries/test_c_filter
clang -o test_binaries/simple_test bindings/c/test/simple_test.c -I bindings/c -L zig-out/lib -lhocdb_c -Wl,-rpath,zig-out/lib
./test_binaries/simple_test
rm test_binaries/simple_test
echo "✅ C Tests passed"

# 8. Run Node.js Tests
echo ""
echo "[8/8] Running Node.js Tests..."
zig build bindings
node bindings/node/test/test.js
node bindings/node/test/test_async_drop.js
echo "✅ Node.js Tests passed"

echo ""
echo "========================================"
echo "🎉 ALL BINDINGS VERIFIED SUCCESSFULLY!"
echo "========================================"
