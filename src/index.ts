import * as zarr from "zarrita/v2";
import { get } from "zarrita/ndarray";
// deno-fmt-ignore
import { consolidated, parseRegion, regionKey, regionToExtent, Shuffle, zip, parseInfo } from "./util";

import type { Async, Readable } from "zarrita";
// deno-fmt-ignore
import type { CoolerDataset, CoolerInfo, Dataset, DataSlice, Extent, Region } from "./types";

// add shuffle codec to registry
zarr.registry.set(Shuffle.codecId, () => Shuffle as any);

export class Indexer1D<Source extends Dataset<any, any>, Field extends keyof Source> {
	constructor(
		public source: Source,
		public fields: Field[],
		private fetcher?: (region: string | Region) => Promise<Extent>,
	) {}

	select<F extends Field>(...fields: F[]) {
		return new Indexer1D(this.source, fields, this.fetcher);
	}

	// Reuse overloads from zarrita's slice fn
	async slice(end: number | null): Promise<DataSlice<Source, Field>>;
	async slice(start: number, end: number | null): Promise<DataSlice<Source, Field>>;
	async slice(
		start: number,
		end: number | null,
		step: number | null,
	): Promise<DataSlice<Source, Field>>;
	async slice(
		start: number | null,
		stop?: number | null,
		step: number | null = null,
	): Promise<DataSlice<Source, Field>> {
		let s = zarr.slice(start as any, stop as any, step);
		let entries = this.fields.map(async (name) => {
			let arr = this.source[name];
			let { data } = await get(arr, [s]);
			return [name, data];
		});
		let obj = await Promise.all(entries).then(Object.fromEntries);
		return this.fields.length === 1 ? obj[this.fields[0]] : obj;
	}

	async fetch(region: string | Region) {
		if (!this.fetcher) throw new Error("No fetcher provided for indexer.");
		let [start, end] = await this.fetcher(region);
		return this.slice(start, end);
	}
}
export class Cooler<Store extends Async<Readable> = Async<Readable>> {
	#chroms?: Record<string, number>;
	#extentCache: Record<string, Extent> = {};

	constructor(
		public readonly info: CoolerInfo,
		public readonly dataset: CoolerDataset<Store>,
	) {}

	get binsize() {
		return this.info["bin-size"] ?? undefined;
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
					let [v] = await indexer.slice(idx, idx + 1);
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
		let nbins = this.dataset.bins.end.shape[0];
		return [nbins, nbins] as const;
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
		return (this.#extentCache[key] = await regionToExtent(this, normed, this.binsize));
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
			grp.attrs().then(parseInfo),
			...paths.map((p) => zarr.get_array(grp, p)),
		]);

		return new Cooler(
			info,
			arrays.reduce((data, arr, i) => {
				let [grp, col] = paths[i].split("/");
				if (!data[grp]) data[grp] = {};
				data[grp][col] = arr;
				return data;
			}, {} as any) as CoolerDataset<Store>,
		);
	}
}
