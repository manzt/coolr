import { searchSorted } from "./util";
import type { Indexer1D } from "./index";
import type { CoolerDataset } from "./types";

function linspace(start: number, stop: number, num: number, endpoint?: boolean) {
	const div = endpoint ? (num - 1) : num;
	const step = (stop - start) / div;
	return Array.from({ length: num }, (_, i) => start + step * i);
}

function unique(arr: number[]): number[] {
	return [...new Set(arr)];
}

function comesBefore(
	a0: number,
	a1: number,
	b0: number,
	b1: number,
	opts: { strict: boolean },
) {
	if (a0 < b0) return opts.strict ? a1 <= b0 : a1 <= b1;
	return false;
}

function contains(
	a0: number,
	a1: number,
	b0: number,
	b1: number,
	opts: { strict: boolean },
) {
	if (a0 > b0 || a1 < b1) return false;
	if (opts.strict && (a0 === b0 || a1 === b1)) return false;
	return a0 <= b0 && a1 >= b1;
}

/**
 *  Take a monotonic sequence of integers and downsample it such that they
 *  are at least ``step`` apart (roughly), preserving the first and last
 *  elements. Returns indices, not values.
 */
function argPrunePartition(arr: number[], step: number) {
	let lo = arr[0];
	let hi = arr[arr.length - 1];
	let num = Math.floor(2 + (hi - lo) / step);
	let cuts = linspace(lo, hi, num);
	return unique(Array.from(cuts, (v) => searchSorted(arr, v)));
}

function getSpans(bbox: BBox, chunksize: number, bin1Offsets: number[]) {
	let [i0, i1, j0, j1] = bbox;
	if ((i1 - i0 < 1) || (j1 - j0 < 1)) {
		return [];
	}
	let edges = argPrunePartition(bin1Offsets.slice(i0, i1 + 1), chunksize);
	return Array.from({ length: edges.length - 1 }, (_, i) => {
		return [edges[i], edges[i + 1]];
	});
}

export type BBox = [i0: number, i1: number, j0: number, j1: number];
export type Fetcher = (field: "count", bbox: BBox) => AsyncIterable<COO<number>>;
export type COO<T> = [row: number, column: number, value: T];

export class CSRReader {
	constructor(
		public pixels: Indexer1D<CoolerDataset["pixels"], "count" | "bin1_id" | "bin2_id">,
		public bin1Offsets: Promise<number[]>,
	) {
		this.read = this.read.bind(this);
	}

	async *read(field: "count", bbox: BBox): AsyncIterable<COO<number>> {
		let [i0, i1, j0, j1] = bbox;
		let [s0, s1] = [i0, i1];

		let offsets = await this.bin1Offsets;
		let offsetLo = offsets[s0];
		let offsetHi = offsets[s1];

		let res = await this.pixels
			.select("bin2_id", field)
			.slice(offsetLo, offsetHi);

		let bin2 = Array.from(res.bin2_id, Number);
		let data = res[field];

		// Now, go row by row, filter out unwanted columns, and accumulate the results.
		for (let i = s0; i < s1; i++) {
			// Shift the global offsets to relative ones.
			let lo = offsets[i] - offsetLo;
			let hi = offsets[i + 1] - offsetLo;

			// Get the j coordinates for this row and filter for the range of desired j values
			for (let j = lo; j < hi; j++) {
				if ((bin2[j] >= j0) && (bin2[j] < j1)) {
					yield [i, bin2[j], data[j]];
				}
			}
		}
	}
}

async function* transpose<T>(coords: AsyncIterable<COO<T>>): AsyncIterable<COO<T>> {
	for await (let [i, j, v] of coords) {
		yield [j, i, v];
	}
}

function swapCoords([i0, i1, j0, j1]: BBox): BBox {
	return [j0, j1, i0, i1];
}

type Field = "count";

function chain<A extends unknown[], B, C>(
	fn1: (...args: A) => B,
	fn2: (_: B) => C,
) {
	return (...args: A) => fn2(fn1(...args));
}

class RangeQuery2D {
	constructor(
		public readonly reader: CSRReader,
		public readonly field: Field,
		public readonly bbox: BBox,
		public readonly reflect: boolean,
	) {}

	entries(): [Fetcher, BBox][] {
		return [[this.reader.read, this.bbox]];
	}

	async *coords() {
		for (let [fetcher, bbox] of this.entries()) {
			for await (let coord of fetcher(this.field, bbox)) {
				yield coord;
			}
		}
	}

	async coo() {
		let coords = [];
		for await (let coord of this.coords()) {
			coords.push(coord);
		}
		return coords;
	}

	async array() {
		let [i0, i1, j0, j1] = this.bbox;
		let [h, w] = [i1 - i0, j1 - j0];

		let data: number[] = Array(h * w).fill(0);
		for await (let [i, j, v] of this.coords()) {
			data[(i - i0) * w + (j - j0)] = v;
		}

		return { data, shape: [h, w] };
	}
}

export class DirectRangeQuery2D extends RangeQuery2D {
	constructor(reader: CSRReader, field: Field, bbox: BBox) {
		super(reader, field, bbox, false);
	}
}

export class FillLowerRangeQuery2D extends RangeQuery2D {
	constructor(reader: CSRReader, field: Field, bbox: BBox) {
		super(reader, field, bbox, true);
	}

	entries(): [Fetcher, BBox][] {
		let { reader, bbox } = this;
		let fetcher = reader.read;

		// If the lower limit of the query exceeds the right limit, we transpose
		// the query bbox to fetch data, then we transpose the result.
		let useTranspose = bbox[1] > bbox[3];
		if (useTranspose) {
			bbox = swapCoords(bbox);
			fetcher = chain(fetcher, transpose);
		}

		let [i0, i1, j0, j1] = bbox;

		// Base cases:
		// Bounding box is anchored on the main diagonal or is completely off
		// the main diagonal.
		if (i0 === j0 || comesBefore(...bbox, { strict: true })) {
			return [[fetcher, bbox]];
		}

		// Mixed case I: partial overlap between i- and j-interval, but not
		// anchored on the main diagonal. Split the query bounding box into two
		// vertically stacked boxes.
		if (comesBefore(...bbox, { strict: false })) {
			return [
				[fetcher, [i0, j0, j0, j1]],
				[fetcher, [j0, i1, j0, j1]],
			];
		}

		// The first block is completely in the lower triangle of the parent
		// matrix, so we query the transpose and transpose the result.
		// However, if we are already transposing, we can remove the
		// operation instead of doing it twice.
		if (contains(j0, j1, i0, i1, { strict: false })) {
			let firstFetcher = useTranspose ? reader.read : chain(reader.read, transpose);
			return [
				[firstFetcher, [j0, i0, i0, i1]],
				[fetcher, [i0, i1, i0, j1]],
			];
		}

		throw new Error("This shouldn't happen.");
	}
}
