# Agent Protocol: Zig Standard Library Documentation

## 1. Context Strategy
**Zig documentation is dynamically generated from source.** There is no static "doc file" to read.
To understand the standard library (`std`), we must read the source code of the installed Zig compiler directly.

* **Rule:** Do not guess `std` APIs. Zig changes frequently.
* **Method:** Use the `scripts/documentify.sh` utility to fetch the exact source definitions for the modules currently being used (e.g., `fs`, `http`, `mem`).

## 2. Generating Context (The "Truth" Source)
We use a script to locate the local Zig installation, copy the relevant `std` modules to a temporary staging area, and pack them into a single XML/Markdown context file.

### A. The Utility Script
Ensure `scripts/documentify.sh` exists and is executable (`chmod +x`).

**`scripts/documentify.sh`**:
```bash
#!/usr/bin/env bash

# PURPOSE: Generates AI-readable context from the local Zig standard library.
# USAGE:   ./documentify.sh [optional_module_list]

# --- DEFAULT MODULES ---
# Modify this list based on the current feature being implemented.
# Common sets:
# - File I/O:  ("fs" "io" "os")
# - Networking: ("net" "http" "uri")
# - Memory:     ("mem" "heap" "array_list")
DEFAULT_MODULES=("fs" "mem" "heap" "array_list" "net")
# -----------------------

MODULES=("${@:-${DEFAULT_MODULES[@]}}")

echo "🔍 Detecting Zig installation..."
if ! command -v zig &> /dev/null; then
    echo "Error: 'zig' command not found."
    exit 1
fi

# Robustly find std_dir (handles Zig struct output)
ZIG_STD_PATH=$(zig env | awk -F'"' '/.std_dir/ {print $2}')

if [ -z "$ZIG_STD_PATH" ] || [ ! -d "$ZIG_STD_PATH" ]; then
    echo "❌ Could not find Zig std lib at: $ZIG_STD_PATH"
    exit 1
fi

echo "✅ Found std lib at: $ZIG_STD_PATH"
echo "📦 Packing modules: ${MODULES[*]}"

# Create temp build dir
mkdir -p _temp_std
cd _temp_std || exit

# Copy modules
for mod in "${MODULES[@]}"; do
    if [ -d "$ZIG_STD_PATH/$mod" ]; then
        cp -R "$ZIG_STD_PATH/$mod" .
    else
        echo "⚠️  Warning: Module '$mod' not found."
    fi
done

# Pack (requires repomix or similar tool)
if command -v npx &> /dev/null; then
    npx repomix . --style xml --output ../zig_context.xml
    echo "✨ Generated: zig_context.xml"
else
    echo "⚠️  Node/npx not found. Using simple concatenation."
    find . -name "*.zig" -exec cat {} + > ../zig_context.txt
    echo "✨ Generated: zig_context.txt"
fi

# Cleanup
cd ..
rm -rf _temp_std