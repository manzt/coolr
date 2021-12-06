import { consolidated, FetchStore, get, get_hierarchy, slice } from "./zarr";
import { ZipFileStore } from "./zip";
import type { AsyncStore } from "zarrita/types";
import type { CoolerDataset, CoolerInfo, SliceData } from "./types";

class Indexer1D<
	Group extends keyof CoolerDataset,
	Cols extends keyof CoolerDataset[Group],
> {
	constructor(
		public data: CoolerDataset,
		public grp: Group,
		public cols: readonly Cols[],
	) {}

	select<Selection extends Cols>(...cols: Selection[]) {
		return new Indexer1D(this.data, this.grp, cols);
	}

	async slice(
		start: number,
		end: number | null,
		step?: number,
	): Promise<SliceData<Group, Cols>>;
	async slice(end: number | null): Promise<SliceData<Group, Cols>>;
	async slice(
		start: number | null,
		end?: number | null,
		step?: number | null,
	): Promise<SliceData<Group, Cols>> {
		let s = slice(start, end, step);
		let entries = this.cols.map(async (name) => {
			let arr = this.data[this.grp][name];
			let { data } = await get(arr as any, [s]) as any;
			return [name, data];
		});
		return Promise.all(entries).then(Object.fromEntries);
	}

	fetch(query: string): Promise<SliceData<Group, Cols>>;
	fetch(
		chrom: string,
		start: number,
		end: number,
	): Promise<SliceData<Group, Cols>>;
	fetch(
		_query: string,
		_start?: number,
		_end?: number,
	): Promise<SliceData<Group, Cols>> {
		console.log("Not implemented.");
		return this.slice(50);
	}
}

export class Cooler<Store extends AsyncStore> {
	constructor(
		public readonly info: CoolerInfo,
		public readonly dataset: CoolerDataset<Store>,
	) {}

	get bins() {
		return new Indexer1D(this.dataset, "bins", ["chrom", "start", "end", "weight"]);
	}

	get pixels() {
		return new Indexer1D(this.dataset, "pixels", ["bin1_id", "bin2_id", "count"]);
	}

	chroms() {
		return new Indexer1D(this.dataset, "chroms", ["name", "length"]).slice(null);
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

	static async fromZarr<Store extends AsyncStore>(store: Store) {
		// https://zarr.readthedocs.io/en/stable/_modules/zarr/convenience.html#consolidate_metadata
		let bytes = await store.get(".zmetadata");
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

		let h = get_hierarchy(store);
		let [info, ...arrays] = await Promise.all([
			h.get_group("/").then((grp) => grp.attrs),
			...paths.map((p) => h.get_array(p)),
		]);

		return new Cooler(
			info as CoolerInfo,
			arrays.reduce((data: any, arr, i) => {
				let [grp, col] = paths[i].split("/");
				if (!data[grp]) data[grp] = {};
				data[grp][col] = arr;
				return data;
			}, {}) as CoolerDataset<Store>,
		);
	}
}

async function run<Store extends AsyncStore>(store: Store, name: string) {
	let c = await Cooler.fromZarr(store);
	console.time(name);

	let pixels = await c.pixels
		.select("count")
		.slice(null);

	let chroms = await c.chroms().then(({ name, length }) => ({
		name: Array.from(name),
		length: Array.from(length),
	}));

	console.timeEnd(name);
	console.log({ pixels, chroms });
	return c;
}

export async function main() {
	// configured only for dev in vite.config.js
	let base = new URL("http://localhost:3000/@data/");
	let input = document.querySelector("input[type=file]")!;

	input.addEventListener("change", async (e: any) => {
		let [file] = e.target.files;
		let store = ZipFileStore.fromBlob(file);
		run(store, "File");
	});

	let c = await run(
		ZipFileStore.fromUrl(new URL("test.10000.zarr.zip", base).href),
		"HTTP-zip",
	);

	let c2 = await run(
		new FetchStore(new URL("test.10000.zarr", base).href),
		"HTTP",
	);
}
