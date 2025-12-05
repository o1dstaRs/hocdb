# For the AI agents to follow

## Agent Conventions

### Test Data Folders
When creating tests for bindings, always use the following naming convention for the data directory:
`b_<language>_test_data`

Examples:
- `b_python_test_data`
- `b_bun_test_data`
- `b_node_test_data`
- `b_c_test_data`
- `b_cpp_test_data`
- `b_go_test_data`

This ensures consistency and makes it easier to clean up test artifacts.

### Verification
On every change to the codebase (especially core engine or bindings), you MUST run the verification script to ensure all bindings are working correctly:

```bash
./verify_all.sh
```

### Running Core Tests
To run the Zig core tests (including integrity and unit tests):
```bash
zig build test --summary all
```

### Benchmarking
When modifying the core engine (`src/root.zig`), you MUST run benchmarks to ensure no performance regressions.

Command:
```bash
zig build bench -Doptimize=ReleaseFast
```

**Target Performance (Apple Silicon M-series):**
| Metric | Target |
| :--- | :--- |
| **Write Throughput** | > 8,000,000 ops/sec |
| **Read Throughput** | > 150,000,000 ops/sec |
| **Aggregation Speed** | > 400,000,000 records/sec |

If performance drops significantly below these targets, investigate immediately.

### Zig Documentation Helper
If you get stuck on Zig specifics or need to check the implementation of standard library modules (e.g., `fs`, `mem`, `heap`), use the `documentify.sh` script.

This script extracts the source code of specified Zig standard library modules and packages them into a single XML context file (`zig_context.xml`) that you can read.

**Usage:**
1.  Edit `documentify.sh` to include the modules you need in the `MODULES` array (default: `fs`, `mem`, `heap`).
2.  Run the script:
    ```bash
    ./documentify.sh
    ```
3.  Read the generated `zig_context.xml` file to understand the Zig implementation.

This is extremely useful for avoiding hallucinations about Zig's standard library.