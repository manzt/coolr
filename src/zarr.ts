import { HTTPRangeReader, unzip, ZipEntry } from "unzipit";
import { registry } from "zarrita";
import ReadOnlyStore from "zarrita/storage/readonly";
import type { AsyncStore } from "zarrita/types";

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

function removePrefix<Key extends string>(
	key: Key,
): Key extends `/${infer Rest}` ? Rest : Key {
	return key[0] === "/" ? key.slice(1) : key as any;
}

export class ZipFileStore extends ReadOnlyStore {
	constructor(public refs: Record<string, ZipEntry>) {
		super();
	}

	async get(key: string) {
		let entry = this.refs[removePrefix(key)];
		if (!entry) return;
		return new Uint8Array(await entry.arrayBuffer());
	}

	async has(key: string) {
		return removePrefix(key) in this.refs;
	}

	static async fromUrl(href: string) {
		let reader = new HTTPRangeReader(href);
		let { entries } = await unzip(reader as any);
		return new ZipFileStore(entries);
	}

	static async fromBlob(blob: Blob) {
		let { entries } = await unzip(blob);
		return new ZipFileStore(entries);
	}
}

export { slice, ZarrArray } from "zarrita";
export { get_hierarchy } from "zarrita/v2";
export { get } from "zarrita/ndarray";
export { default as FetchStore } from "zarrita/storage/fetch";

type ConsolidatedMetadata = Record<string, Record<string, any>>;

export function withConsolidatedMetadata<Store extends AsyncStore>(
	store: Store,
	meta: ConsolidatedMetadata,
) {
	let encoder = new TextEncoder();
	let get = (target: Store, prop: string) => {
		if (prop === "get") {
			return (key: string) => {
				let prefix = key[0] === "/" ? key.slice(1) : key;
				if (prefix in meta) {
					let str = JSON.stringify(meta[prefix]);
					return Promise.resolve(encoder.encode(str));
				}
				return target.get(key);
			};
		}

		if (prop === "has") {
			return (key: string) => {
				let prefix = key[0] === "/" ? key.slice(1) : key;
				if (prefix in meta) return Promise.resolve(true);
				return target.has(key);
			};
		}

		return Reflect.get(target, prop);
	};

	return new Proxy(store, { get });
}

export async function getJson<T = any>(store: AsyncStore, path: string) {
	let bytes = await store.get(path);
	if (!bytes) throw new Error("no json");
	return JSON.parse(new TextDecoder().decode(bytes)) as T;
}
