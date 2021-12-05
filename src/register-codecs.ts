import { registry } from "zarrita";

type Importer = Parameters<typeof registry["set"]>[1];

function unwrap(fn: () => Promise<{ default: unknown }>) {
	return (() => fn().then((m) => m.default)) as Importer;
}

// need to keep string literal import() use so that module resolution works
registry.set("blosc", unwrap(() => import("numcodecs/blosc")));
registry.set("gzip", unwrap(() => import("numcodecs/gzip")));
registry.set("zlib", unwrap(() => import("numcodecs/zlib")));
registry.set("zstd", unwrap(() => import("numcodecs/zstd")));
registry.set("lz4", unwrap(() => import("numcodecs/lz4")));
