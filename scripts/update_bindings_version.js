import fs from "fs";
import path from "path";

const HOCDB_ROOT = path.resolve(import.meta.dir, "..");
const BINDINGS_DIR = path.join(HOCDB_ROOT, "bindings");

function updatePackageJson(filePath, newVersion) {
    if (!fs.existsSync(filePath)) {
        console.warn(`⚠️ File not found: ${filePath}`);
        return;
    }

    try {
        const pkg = JSON.parse(fs.readFileSync(filePath, "utf8"));
        if (pkg.version !== newVersion) {
            console.log(`Updating ${filePath}: ${pkg.version} -> ${newVersion}`);
            pkg.version = newVersion;
            fs.writeFileSync(filePath, JSON.stringify(pkg, null, 2) + "\n");
        } else {
            console.log(`✓ ${filePath} is already at ${newVersion}`);
        }
    } catch (e) {
        console.error(`❌ Failed to update ${filePath}:`, e.message);
        process.exit(1);
    }
}

function main() {
    const rootPkgPath = path.join(HOCDB_ROOT, "package.json");
    if (!fs.existsSync(rootPkgPath)) {
        console.error(`❌ Root package.json not found at ${rootPkgPath}`);
        process.exit(1);
    }

    const rootPkg = JSON.parse(fs.readFileSync(rootPkgPath, "utf8"));
    const newVersion = rootPkg.version;

    if (!newVersion) {
        console.error("❌ Version not found in root HOCDB package.json");
        process.exit(1);
    }

    console.log(`Syncing HOCDB version: ${newVersion}`);

    // Update Bindings
    const bindingsToUpdate = ["bun", "node"];

    for (const binding of bindingsToUpdate) {
        const pkgPath = path.join(BINDINGS_DIR, binding, "package.json");
        updatePackageJson(pkgPath, newVersion);
    }
}

main();
