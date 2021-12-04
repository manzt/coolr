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

class RangeIndexer<Group extends String, Cols extends string[]> {
	constructor(
		public cooler: Cooler,
		public grp: Group,
		public cols: Cols,
	) {}

	select(...cols: Cols[number][]) {
		return new RangeIndexer(this.cooler, this.grp, cols);
	}

	async slice(..._: any[]) {
		let s = zarr.slice.apply(null, arguments);
		let entries = this.cols.map(async (name) => {
			let a = await this.cooler.root.get_array(`${this.grp}/${name}`);
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

class Bins extends RangeIndexer<"bins", ["chrom", "start", "end", "weight"]> {}

class Pixels extends RangeIndexer<"pixels", ["bin1_id", "bin2_id", "count"]> {}

export class Cooler {
	constructor(public root: zarr.Group, public info: CoolerInfo) {}

	get bins() {
		return new Bins(this, "bins", ["chrom", "start", "end", "weight"]);
	}

	get pixels() {
		return new Pixels(this, "pixels", ["bin1_id", "bin2_id", "count"]);
	}

	static async fromZarr(grp: zarr.Group) {
		return new Cooler(grp, await grp.attrs);
	}
}

async function run(store: any, name: string) {
	console.time(name)
	let grp = await zarr.v2.get_hierarchy(store).get_group("/");
	let c = await Cooler.fromZarr(grp);
	await c.pixels.select("count").slice(null).then(console.log)
	console.timeEnd(name);
}

export async function main() {
	// let href = "http://localhost:8080/test.10000.zarr.zip";
	let input = document.querySelector("input[type=file]")!;

	input.addEventListener('change', async (e: any) => {
		let store = await ZipFileStore.fromBlob(e.target.files[0]);
		run(store, 'File');
	});

	let href = "http://localhost:8080/test.10000.zarr.zip";
	let store = await ZipFileStore.fromUrl(href);
	run(store, 'HTTP');
}
