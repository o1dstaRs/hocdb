#!/usr/bin/env bash

# This script generates an AI-ready context file from the Zig standard library by dynamically
# locating your installation and staging requested modules locally. It is essential because
# packing tools like 'repomix' often fail on absolute system paths; by temporarily copying
# modules (e.g., 'fs') to the workspace, this script allows you to feed the exact source
# definitions of your Zig version to an LLM, ensuring accurate, hallucination-free coding assistance.

# --- CONFIGURATION ---
MODULES=("fs" "mem" "heap")
# ---------------------

echo "🔍 Detecting Zig installation..."

# 1. Check for Zig
if ! command -v zig &> /dev/null; then
    echo "Error: 'zig' command not found."
    exit 1
fi

# 2. Extract std_dir using awk (parsing Zig struct syntax)
# Looks for line like: .std_dir = "/path/to/std",
ZIG_STD_PATH=$(zig env | awk -F'"' '/.std_dir/ {print $2}')

# 3. Validate path
if [ -z "$ZIG_STD_PATH" ] || [ ! -d "$ZIG_STD_PATH" ]; then
    echo "❌ Could not find Zig standard library path."
    echo "Debug: Parsed path was '$ZIG_STD_PATH'"
    exit 1
fi

echo "✅ Found std lib at: $ZIG_STD_PATH"
echo "📦 Copying modules: ${MODULES[*]}..."

# 4. Copy modules to current directory
for mod in "${MODULES[@]}"; do
    if [ -d "$ZIG_STD_PATH/$mod" ]; then
        cp -R "$ZIG_STD_PATH/$mod" .
    else
        echo "⚠️  Warning: Module '$mod' not found in standard library."
    fi
done

# 5. Run Repomix
echo "🤖 Running repomix..."
# Using --style xml for better AI context parsing
npx repomix "${MODULES[@]}" --style xml --output zig_context.xml

# 6. Cleanup
echo "🧹 Cleaning up temporary files..."
rm -rf "${MODULES[@]}"

echo "✨ Done! Context saved to zig_context.xml"
