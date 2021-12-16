import * as zarr from "zarrita/v2";
import { get } from "zarrita/ndarray";
import {
	consolidated,
	parseRegion,
	regionKey,
	regionToExtent,
	Shuffle,
	zip,
} from "./util";

import type { Async, Readable } from "zarrita";
import type {
	CoolerDataset,
	CoolerInfo,
	Dataset,
	DataSlice,
	Extent,
	Region,
} from "./types";

// add shuffle codec to registry
zarr.registry.set(Shuffle.codecId, () => Shuffle as any);

export class Indexer1D<Source extends Dataset<any, any>, Fields extends keyof Source> {
	constructor(
		public source: Source,
		public fields: Fields[],
		private fetcher?: (region: string | Region) => Promise<Extent>,
	) {}

	select<F extends typeof this.fields[number]>(...fields: F[]) {
		return new Indexer1D(this.source, fields, this.fetcher);
	}

	// Reuse overloads from zarrita's slice fn
	async slice(end: number | null): Promise<DataSlice<Source, Fields>>;
	async slice(start: number, end: number | null): Promise<DataSlice<Source, Fields>>;
	async slice(
		start: number,
		end: number | null,
		step: number | null,
	): Promise<DataSlice<Source, Fields>>;
	async slice(
		start: number | null,
		stop?: number | null,
		step: number | null = null,
	): Promise<DataSlice<Source, Fields>> {
		let s = zarr.slice(start as any, stop as any, step);
		let entries = this.fields.map(async (name) => {
			let arr = this.source[name];
			let { data } = await get(arr, [s]);
			return [name, data];
		});
		return Promise.all(entries).then(Object.fromEntries);
	}

	async fetch(region: string | Region) {
		if (!this.fetcher) throw new Error("No fetcher provided for indexer.");
		let [start, end] = await this.fetcher(region);
		return this.slice(start, end);
	}
}
export class Cooler<Store extends Async<Readable>> {
	#chroms?: Record<string, number>;
	#extentCache: Record<string, Extent> = {};

	constructor(
		public readonly info: CoolerInfo,
		public readonly dataset: CoolerDataset<Store>,
	) {}

	get binsize() {
		return this.info["bin-size"];
	}

	get bins() {
		return new Indexer1D(
			this.dataset.bins,
			["chrom", "start", "end", "weight"],
			(region) => this.extent(region),
		);
	}

	get pixels() {
		return new Indexer1D(
			this.dataset.pixels,
			["bin1_id", "bin2_id", "count"],
			async (region) => {
				let [i0, i1] = await this.extent(region);
				let indexer = this.indexes.select("bin1_offset");
				let fetchAndParse = async (idx: number) => {
					let { bin1_offset: [v] } = await indexer.slice(idx, idx + 1);
					return Number(v);
				};
				return Promise.all([
					fetchAndParse(i0),
					fetchAndParse(i1),
				]);
			},
		);
	}

	get indexes() {
		return new Indexer1D(this.dataset.indexes, ["chrom_offset", "bin1_offset"]);
	}

	get shape() {
		return [this.info.nbins, this.info.nbins] as const;
	}

	async chroms() {
		if (this.#chroms) return this.#chroms;
		let indexer = new Indexer1D(this.dataset.chroms, ["name", "length"]);
		let { name, length } = await indexer.slice(null);
		return (this.#chroms = Object.fromEntries(
			zip(Array.from(name), Array.from(length)),
		));
	}

	// https://cooler.readthedocs.io/en/latest/api.html#cooler-class
	chromnames() {
		return this.chroms().then(Object.keys);
	}

	chromsizes() {
		return this.chroms().then(Object.values);
	}

	async offset(region: string | Region): Promise<number> {
		return this.extent(region).then(([offset]) => offset);
	}

	async extent(region: string | Region): Promise<Extent> {
		let normed = parseRegion(region, await this.chroms());
		let key = regionKey(normed);
		if (key in this.#extentCache) {
			return this.#extentCache[key];
		}
		return (this.#extentCache[key] = await regionToExtent(this, normed));
	}

	matrix() {/* TODO */}

	static async open<Store extends Async<Readable>>(
		store: Store,
		path: `/${string}` = "/",
	) {
		// https://zarr.readthedocs.io/en/stable/_modules/zarr/convenience.html#consolidate_metadata
		let meta_key = `${
			path.endsWith("/") ? path : `${path}/` as const
		}.zmetadata` as const;
		let bytes = await store.get(meta_key);
		if (bytes) {
			let str = new TextDecoder().decode(bytes);
			store = consolidated(store, JSON.parse(str));
		}

		let paths = [
			"bins/chrom",
			"bins/start",
			"bins/end",
			"bins/weight",
			"chroms/name",
			"chroms/length",
			"indexes/bin1_offset",
			"indexes/chrom_offset",
			"pixels/bin1_id",
			"pixels/bin2_id",
			"pixels/count",
		] as const;

		let grp = await zarr.get_group(store, path);
		let [info, ...arrays] = await Promise.all([
			grp.attrs(),
			...paths.map((p) => zarr.get_array(grp, p)),
		]);
		if (info["creation-date"]) {
			info["creation-date"] = new Date(info["creation-date"]);
		}
		if (info["metadata"]) {
			info["metadata"] = JSON.parse(info["metadata"]);
		}
		let dset = arrays.reduce((data: any, arr, i) => {
			let [grp, col] = paths[i].split("/");
			if (!data[grp]) data[grp] = {};
			data[grp][col] = arr;
			return data;
		}, {}) as CoolerDataset<Store>;
		return new Cooler(info as CoolerInfo, dset);
	}
}

async function run<Store extends Async<Readable>>(
	store: Store,
	name: string,
	path?: `/${string}`,
) {
	let c = await Cooler.open(store, path);
	console.time(name);

	let pixels = await c.pixels
		.select("count")
		.slice(10);

	let chroms = await c.chroms();
	console.log(chroms);

	console.timeEnd(name);
	console.log({ pixels, chroms });
	return c;
}

export async function main() {
	let [
		{ default: _FetchStore },
		{ default: ReferenceStore },
		{ default: ZipFileStore },
	] = await Promise.all([
		import("zarrita/storage/fetch"),
		import("zarrita/storage/ref"),
		import("zarrita/storage/zip"),
	]);

	// configured only for dev in vite.config.js
	let base = new URL("http://localhost:3000/@data/");
	let input = document.querySelector("input[type=file]")!;

	input.addEventListener("change", async (e: any) => {
		let [file] = e.target.files;
		let store = ZipFileStore.fromBlob(file);
		run(store, "File");
	});

	// let c = await run(ZipFileStore.fromUrl(new URL("test.10000.zarr.zip", base).href), "HTTP-zip");
	// let c1 = await run(new FetchStore(new URL("test.10000.zarr", base).href), "HTTP");

	let c = await run(
		await ReferenceStore.fromUrl(new URL("test.mcool.remote.json", base)),
		"hdf5",
		"/resolutions/10000",
	);

	console.time("first");
	console.log(await c.extent("chr17:82,200,000-83,200,000"));
	console.timeEnd("first");

	console.time("second");
	console.log(await c.extent("chr17:82,200,000-83,200,000"));
	console.timeEnd("second");

	console.log(c);
}
