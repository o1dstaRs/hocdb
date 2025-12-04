const path = require('path');
const fs = require('fs');

// Try to find the built binary
const buildPath = path.join(__dirname, '..', '..', 'zig-out', 'lib', 'libhocdb.dylib'); // macOS
// Note: On Linux it would be .so, on Windows .dll.
// For a real package, we'd rename it to .node or use node-gyp/cmake-js.
// But for this setup, we'll just load the dylib if Node allows it, or rename it.

// Node.js requires .node extension for native modules usually.
// Let's try to load it. If it fails, we might need to copy/rename.

let bindingPath = buildPath;
if (!fs.existsSync(bindingPath)) {
    // Fallback to local build dir if running from source (Linux)
    bindingPath = path.join(__dirname, '..', '..', 'zig-out', 'lib', 'libhocdb.so');
}

if (!fs.existsSync(bindingPath)) {
    console.error("Could not find hocdb native binding at", bindingPath);
    process.exit(1);
}

// We can use 'process.dlopen' or just require if it has .node extension.
// Since it's .dylib/.so, we might need to symlink it to .node
const nodePath = path.join(__dirname, 'hocdb.node');
try {
    if (fs.existsSync(nodePath)) fs.unlinkSync(nodePath);
    fs.copyFileSync(bindingPath, nodePath);
} catch (e) {
    // Ignore if we can't copy (maybe permission or already exists)
}

const addon = require(nodePath);

module.exports = addon;
