import * as zarr from "zarrita/v2";
import { get } from "zarrita/ndarray";
// deno-fmt-ignore
import { consolidated, parseRegion, regionKey, regionToExtent, Shuffle, zip, parseInfo } from "./util";
import { BBox, CSRReader, FillLowerRangeQuery2D } from "./ranges";

import type { Async, Readable } from "zarrita";
// deno-fmt-ignore
import type { CoolerDataset, CoolerInfo, Dataset, DataSlice, Extent, Region } from "./types";
export type { CoolerDataset, CoolerInfo };

// add shuffle codec to registry
zarr.registry.set(Shuffle.codecId, () => Shuffle as any);

export class Indexer1D<Source extends Dataset<any, any>, Field extends keyof Source> {
	constructor(
		public readonly source: Source,
		public readonly fields: Field[],
		private readonly fetcher?: (region: string | Region) => Promise<Extent>,
	) {}

	select<F extends Field>(...fields: F[]) {
		return new Indexer1D(this.source, fields, this.fetcher);
	}

	async slice(end: number | null): Promise<DataSlice<Source, Field>>;
	async slice(start: number, end: number | null): Promise<DataSlice<Source, Field>>;
	async slice(a: any, b?: any): Promise<DataSlice<Source, Field>> {
		let entries = await Promise.all(
			this.fields.map(async (name) => {
				let arr = this.source[name];
				let { data } = await get(arr, [zarr.slice(a, b)]);
				return [name, data];
			}),
		);
		let obj = Object.fromEntries(entries);
		return this.fields.length === 1 ? obj[this.fields[0]] : obj;
	}

	async fetch(region: string | Region) {
		if (!this.fetcher) throw new Error("No fetcher provided for indexer.");
		let [start, end] = await this.fetcher(region);
		return this.slice(start, end);
	}
}

type Fetcher2D = (region: string | Region, region2?: string | Region) => Promise<BBox>;

export class Matrix {
	constructor(
		public readonly cooler: Cooler,
		public readonly reader: CSRReader,
		private readonly fetcher: Fetcher2D,
	) {}

	get shape() {
		return this.cooler.shape;
	}

	async slice(i0: number, i1: number, j0: number, j1: number) {
		const field = "count";
		const balance = "weight";

		let bbox: BBox = [i0, i1, j0, j1];
		let engine = new FillLowerRangeQuery2D(this.reader, field, bbox);

		let arr = await engine.array();

		if (balance) {
			let weights = this.cooler.bins.select(balance);
			let bias1 = await weights.slice(i0, i1);
			let bias2 = (i0 === j0 && i1 === j1) ? bias1 : await weights.slice(j0, j1);
			for (let i = 0; i < (i1 - i0); i++) {
				for (let j = 0; j < (j1 - j0); j++) {
					let offset = i * arr.shape[1] + j;
					arr.data[offset] = arr.data[offset] * bias1[i] * bias2[j];
				}
			}
		}

		return arr;
	}

	async fetch(region: string | Region, region2?: string | Region) {
		let [i0, i1, j0, j1] = await this.fetcher(region, region2);
		return this.slice(i0, i1, j0, j1);
	}
}

export class Cooler<Store extends Async<Readable> = Async<Readable>> {
	#cachedChroms?: Record<string, number>;
	#cachedBin1Offsets?: Promise<number[]>;
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
			this.extent,
		);
	}

	get indexes() {
		return new Indexer1D(this.dataset.indexes, ["bin1_offset", "chrom_offset"]);
	}

	get pixels() {
		return new Indexer1D(
			this.dataset.pixels,
			["bin1_id", "bin2_id", "count"],
			async (region) => {
				let [[i0, i1], offsets] = await Promise.all([
					this.extent(region),
					this.#bin1Offsets,
				]);
				return [offsets[i0], offsets[i1]];
			},
		);
	}

	get #bin1Offsets() {
		if (this.#cachedBin1Offsets) return this.#cachedBin1Offsets;
		return (this.#cachedBin1Offsets = this.indexes
			.select("bin1_offset")
			.slice(null)
			.then((b) => Array.from(b, Number)));
	}

	get shape() {
		let nbins = this.dataset.bins.end.shape[0];
		return [nbins, nbins] as const;
	}

	async chroms() {
		if (this.#cachedChroms) return this.#cachedChroms;
		let indexer = new Indexer1D(this.dataset.chroms, ["name", "length"]);
		let { name, length } = await indexer.slice(null);
		return (this.#cachedChroms = Object.fromEntries(
			zip(Array.from(name), length),
		));
	}

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

	get matrix() {
		let reader = new CSRReader(this.pixels, this.#bin1Offsets);
		return new Matrix(this, reader, async (region, region2) => {
			let [[i0, i1], [j0, j1]] = await Promise.all([
				this.extent(region),
				this.extent(region2 ?? region),
			]);
			return [i0, i1, j0, j1];
		});
	}
}

export async function open<Store extends Async<Readable>>(
	store: Store,
	path: `/${string}` = "/",
) {
	// https://zarr.readthedocs.io/en/stable/_modules/zarr/convenience.html#consolidate_metadata
	let metaKey = `${path.endsWith("/") ? path : `${path}/` as const}.zmetadata` as const;
	let bytes = await store.get(metaKey);
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
