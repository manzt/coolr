import { registry } from "zarrita";
import type { AbsolutePath, Async, Readable } from "zarrita";

export { slice, ZarrArray } from "zarrita";
export { get_hierarchy } from "zarrita/v2";
export { get } from "zarrita/ndarray";

import { stripPrefix } from "./storage";

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

type ConsolidatedMetadata = {
	metadata: Record<string, Record<string, any>>;
	zarr_consolidated_format: 1;
};

/**
 * Proxies requests to the underlying store.
 */
export function consolidated<
	Store extends Async<Readable>,
>(
	store: Store,
	{ metadata }: ConsolidatedMetadata,
) {
	let encoder = new TextEncoder();
	let get = (target: Store, prop: string) => {
		if (prop === "get") {
			return (key: AbsolutePath) => {
				let prefix = stripPrefix(key);
				if (prefix in metadata) {
					let str = JSON.stringify(metadata[prefix]);
					return Promise.resolve(encoder.encode(str));
				}
				return target.get(key);
			};
		}
		return Reflect.get(target, prop);
	};

	return new Proxy(store, { get });
}
