#!/bin/bash

# Define paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOCDB_ROOT="$(dirname "$SCRIPT_DIR")"
BINDINGS_DIR="$HOCDB_ROOT/bindings"
ROOT_PKG_JSON="$HOCDB_ROOT/package.json"

# Check if root package.json exists
if [ ! -f "$ROOT_PKG_JSON" ]; then
    echo "❌ Root package.json not found at $ROOT_PKG_JSON"
    exit 1
fi

# Extract version from root package.json
# Using simple grep/sed/awk to avoid jq dependency if possible, assuming standard formatting
NEW_VERSION=$(grep '"version":' "$ROOT_PKG_JSON" | head -1 | awk -F: '{ print $2 }' | sed 's/[", ]//g')

if [ -z "$NEW_VERSION" ]; then
    echo "❌ Version not found in root HOCDB package.json"
    exit 1
fi

echo "Syncing HOCDB version: $NEW_VERSION"

# Function to update package.json
update_package_json() {
    local target_file=$1
    local new_ver=$2

    if [ ! -f "$target_file" ]; then
        echo "⚠️ File not found: $target_file"
        return
    fi
    
    # Read current version
    local current_ver=$(grep '"version":' "$target_file" | head -1 | awk -F: '{ print $2 }' | sed 's/[", ]//g')
    
    if [ "$current_ver" != "$new_ver" ]; then
        echo "Updating $target_file: $current_ver -> $new_ver"
        # Use sed to replace version. 
        # Note: minimal match to ensure we only replace the version field
        # We use a temp file for cross-platform compatibility (BSD sed vs GNU sed)
        sed "s/\"version\": \".*\"/\"version\": \"$new_ver\"/" "$target_file" > "${target_file}.tmp" && mv "${target_file}.tmp" "$target_file"
    else
        echo "✓ $target_file is already at $new_ver"
    fi
}

# Update Bindings
BINDINGS=("bun" "node")

for binding in "${BINDINGS[@]}"; do
    PKG_PATH="$BINDINGS_DIR/$binding/package.json"
    update_package_json "$PKG_PATH" "$NEW_VERSION"
done
