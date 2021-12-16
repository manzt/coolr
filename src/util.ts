import { slice } from "zarrita/v2";
import { get } from "zarrita/ndarray";

import type { AbsolutePath, Async, Readable } from "zarrita";
import type { Codec } from "numcodecs";
import type { Dataset, DataSlice, Extent, NormedRegion, Region } from "./types";
import type { Cooler } from "./index";

export function zip<A, B>(a: A[], b: B[]): [A, B][] {
	return a.map((x, i) => [x, b[i]]);
}

export function regionKey(reg: NormedRegion): string {
	return reg.join("-");
}

type ConsolidatedMetadata = {
	metadata: Record<string, Record<string, any>>;
	zarr_consolidated_format: 1;
};

/** Proxies requests to the underlying store. */
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
				let prefix = key.slice(1);
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

export class Shuffle implements Codec {
	static codecId = "shuffle";
	public elementsize: number;

	constructor({ elementsize }: { elementsize: number }) {
		this.elementsize = elementsize;
	}

	static fromConfig(config: { id: "shuffle"; elementsize: number }) {
		return new Shuffle(config);
	}

	encode(): never {
		throw new Error("encode not implemented for `shuffle` codec.");
	}

	// https://github.com/zarr-developers/numcodecs/blob/500c048d9de1236e9bcd52c7065dd14f4bb09a89/numcodecs/_shuffle.pyx#L21-L31
	decode(bytes: Uint8Array): Uint8Array {
		if (this.elementsize <= 1) {
			return bytes;
		}
		let out = new Uint8Array(bytes.length);
		let count = Math.floor(bytes.length / this.elementsize);
		let offset = 0;
		for (let i = 0; i < this.elementsize; i++) {
			offset = i * count;
			for (let byte_index = 0; byte_index < count; byte_index++) {
				out[
					byte_index * this.elementsize + i
				] = bytes[offset + byte_index];
			}
		}
		return out;
	}
}

function parseRegionStr(str: string): Region {
	let fullMatch = str.match(/(.*):(.*)-(.*)/);
	if (fullMatch) {
		let [_, chr, start, end] = fullMatch;
		return [
			chr,
			start ? Number(start.replaceAll(",", "")) : null,
			end ? Number(end.replaceAll(",", "")) : null,
		];
	}
	let chrAndStart = str.match(/(.*):(.*)/);
	if (chrAndStart) {
		let [_, chr, start] = chrAndStart;
		return [chr, start ? Number(start.replaceAll(",", "")) : null, null];
	}
	return [str, null, null];
}

export function parseRegion(
	region: string | Region,
	chromsizes: Record<string, number>,
): NormedRegion {
	let [chr, start, end] = typeof region === "string" ? parseRegionStr(region) : region;
	let clen = chromsizes[chr];
	if (!clen) {
		throw new Error(`Unknown seequence label: ${chr}.`);
	}
	let numStart = start ?? 0;
	let numEnd = end ?? clen;
	if (numEnd < numStart) {
		throw new Error("End cannot be less than start");
	}
	if (numStart < 0 || (numEnd > clen)) {
		throw new Error(`Genomic region out of bounds: [${numStart}, ${numEnd})`);
	}
	return [chr, numStart, numEnd];
}

export async function regionToExtent(
	cooler: Cooler<any>,
	region: NormedRegion,
	binsize?: number,
): Promise<Extent> {
	let chromIds = Object.fromEntries(
		(await cooler.chromnames()).map((n, i) => [n, i]),
	);
	let [chrom, start, end] = region;
	let cid = chromIds[chrom];
	let indexer = cooler.indexes.select("chrom_offset");

	if (binsize) {
		let { chrom_offset: [offset] } = await indexer.slice(cid, cid + 1);
		return [
			Number(offset) + Math.floor(start / binsize),
			Number(offset) + Math.ceil(end / binsize),
		];
	}
	let { chrom_offset: [bigintLo, bigintHi] } = await indexer.slice(cid, cid + 2);
	let chromLo = Number(bigintLo);
	let chromHi = Number(bigintHi);

	let { start: chromBins } = await cooler
		.bins
		.select("start")
		.slice(chromLo, chromHi);

	return [
		chromLo + searchSorted(chromBins, start, "right") - 1,
		chromLo + searchSorted(chromBins, end, "left"),
	];
}

// binary search to eagerly find insert
function findInsert(arr: ArrayLike<number>, value: number) {
	let start = 0;
	let end = arr.length - 1;
	while (start <= end) {
		let mid = Math.floor((start + end) / 2);
		if (arr[mid] === value) {
			return mid;
		} else if (arr[mid] < value) {
			start = mid + 1;
		} else {
			end = mid - 1;
		}
	}
	return end + 1;
}

function searchSorted(
	arr: ArrayLike<number>,
	value: number,
	side: "left" | "right" = "left",
) {
	let idx = findInsert(arr, value);
	if (side === "left") {
		while (arr[idx - 1] === value) idx--;
	} else {
		while (arr[idx] === value) idx++;
	}
	return idx;
}
