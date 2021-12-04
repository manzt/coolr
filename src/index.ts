// @ts-ignore
import * as zarr from "zarrita";
// @ts-ignore
import { get } from "zarrita/ndarray";
// @ts-ignore
import HTTPStore from "zarrita/storage/http";

import { ZipFileStore } from "./zip";

// add codecs to registry
for (let id of ["gzip", "zlib", "zstd", "lz4", "blosc"]) {
	let base = "https://cdn.skypack.dev/numcodecs/";
	let href = base + id;
	zarr.registry.set(id, () => import(href).then((m) => m.default));
}

type Store = any;

interface CoolerData {
	info: CoolerInfo;
	bins: {
		chrom: zarr.ZarrArray;
		end: zarr.ZarrArray;
		start: zarr.ZarrArray;
		weight: zarr.ZarrArray; // optional, includes meta
	};
	chroms: {
		name: string[];
		length: number[];
	};
	indexes: {
		bin1_offset: zarr.ZarrArray;
		chrom_offset: zarr.ZarrArray;
	};
	pixels: {
		bin1_id: zarr.ZarrArray;
		bin2_id: zarr.ZarrArray;
		count: zarr.ZarrArray;
	};
}

async function getCodec(config?: Record<string, any>) {
	if (!config) return;
	let importer = zarr.registry.get(config.id);
	if (!importer) throw new Error("missing codec" + config.id);
	let ctr = await importer();
	return ctr.fromConfig(config);
}

let keyPrefix = (path: string) => path.length > 1 ? path + "/" : "";

let chunkKey = (path: string, chunk_separator: "." | "/") => {
	let prefix = keyPrefix(path);
	return (chunk_coords: number[]) => {
		let chunk_identifier = chunk_coords.join(chunk_separator);
		let chunk_key = prefix + chunk_identifier;
		return chunk_key;
	};
};

async function loadGroup<Name extends string, Key extends string>(
	store: Store,
	metadata: Record<string, any>,
	name: Name,
	keys: readonly Key[],
): Promise<Record<Key, zarr.ZarrArray>> {
	let nodes = keys.map(async (key) => {
		let path = `${name}/${key}`;
		let meta = metadata[path + "/.zarray"];
		let arr = new zarr.ZarrArray({
			store,
			path,
			shape: meta.shape,
			dtype: meta.dtype,
			chunk_shape: meta.chunks,
			chunk_key: chunkKey(path, meta.dimension_separator ?? "."),
			compressor: await getCodec(meta.compressor),
			fill_value: meta.fill_value,
			attrs: metadata[path + "/.zattrs"] ?? {},
		});
		return [key, arr];
	});
	return Object.fromEntries(await Promise.all(nodes));
}

async function openConsolidated(store: Store, path = ""): Promise<CoolerData> {
	let buf = await store.get(path + "/.zmetadata");
	let { metadata } = JSON.parse(new TextDecoder().decode(buf));
	let entries = ([
		["bins", ["chrom", "end", "start", "weight"]],
		["chroms", ["name", "length"]],
		["indexes", ["bin1_offset", "chrom_offset"]],
		["pixels", ["bin1_id", "bin2_id", "count"]],
	] as const).map(async ([name, keys]) => [name, await loadGroup(store, metadata, name, keys)]);
	return {
		...Object.fromEntries(await Promise.all(entries)),
		info: metadata[".zattrs"],
	};
}

interface CoolerInfo {
	format: string;
	"format-version": number;
	"bin-type": "fixed" | "variable";
	"bin-size": number;
	"storage-mode": "symmetric-upper" | "square";
	nbins: number;
	chroms: number;
	nnz: number;
	assembly: string | null;
	"generated-by"?: string;
	"creation-date"?: string;
	metadata?: string;
}

class RangeIndexer<
	Group extends keyof CoolerData,
	Cols extends keyof CoolerData[Group],
> {
	constructor(
		public data: CoolerData,
		public grp: Group,
		public cols: readonly Cols[],
	) {}

	select(...cols: Cols[]) {
		return new RangeIndexer(this.data, this.grp, cols);
	}

	async slice(..._: any[]) {
		let s = zarr.slice.apply(null, arguments);
		let entries = this.cols.map(async (name) => {
			let a = await this.data[this.grp][name];
			let { data } = await get(a, [s]);
			return [name, data];
		});
		return Promise.all(entries).then(Object.fromEntries);
	}

	fetch(query: string): any;
	fetch(chrom: string, start: number, end: number): any;
	fetch(_query: string, _start?: number, _end?: number) {
		console.log("Not implemented.");
		return this.slice(50);
	}
}

export class Cooler {
	constructor(
		public data: CoolerData,
	) {}

	get info() {
		return this.data.info;
	}

	get bins() {
		return new RangeIndexer(this.data, "bins", ["chrom", "start", "end", "weight"]);
	}

	get pixels() {
		return new RangeIndexer(this.data, "pixels", ["bin1_id", "bin2_id", "count"]);
	}

	chroms() {
		return Promise.all([
			get(this.data.chroms.name, null),
			get(this.data.chroms.length, null),
		]);
	}

	// https://cooler.readthedocs.io/en/latest/api.html#cooler-class
	get chromnames() {
		/* TODO */ return undefined;
	}
	get chromsizes() {
		/* TODO */ return undefined;
	}
	get binsize() {
		/* TODO */ return undefined;
	}
	extent() {/* TODO */}
	offset() {/* TODO */}
	matrix() {/* TODO */}

	static async fromZarr(store: Store) {
		let data = await openConsolidated(store);
		return new Cooler(data);
	}
}

async function run(store: any, name: string) {
	let c = await Cooler.fromZarr(store);
	console.time(name);
	await c.pixels
		.select("count", "bin2_id", "bin1_id")
		.slice(30)
		.then(console.log);

	c.chroms().then(console.log);
	console.timeEnd(name);
}

export async function main() {
	let input = document.querySelector("input[type=file]")!;

	input.addEventListener("change", async (e: any) => {
		let [file] = e.target.files;
		let store = await ZipFileStore.fromBlob(file);
		run(store, "File");
	});

	let href = "http://localhost:8080/test.10000.zarr.zip";
	let store = await ZipFileStore.fromUrl(href);
	run(store, "HTTP");
}
