import type { Indexer1D } from "./index";
import type { CoolerDataset } from "./types";

import { searchSorted } from "./util";

function linspace(start: number, stop: number, num: number, endpoint?: boolean) {
	const div = endpoint ? (num - 1) : num;
	const step = (stop - start) / div;
	return Array.from({ length: num }, (_, i) => start + step * i);
}

function unique(arr: number[]): number[] {
	return [...new Set(arr)];
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

export type BBox = [i0: number, i1: number, j0: number, j1: number];

class CSRReader {
	constructor(
		public pixels: Indexer1D<CoolerDataset["pixels"], "count" | "bin1_id" | "bin2_id">,
		public bin1Offsets: Promise<number[]>,
	) {
	}

	async getSpans([i0, i1, j0, j1]: BBox, chunksize: number) {
		let edges: number[];
		if ((i1 - i0 < 1) || (j1 - j0 < 1)) {
			edges = [];
		} else {
			let offsets = await this.bin1Offsets;
			edges = argPrunePartition(
				offsets.slice(i0, i1 + 1),
				chunksize,
			);
		}
		return Array.from({ length: edges.length - 1 }, (_, i) => {
			return [edges[i], edges[i + 1]] as const;
		});
	}

	async read(
		field: "count",
		[i0, i1, j0, j1]: BBox,
		opts: {
			rowSpan?: [number, number];
			reflect?: boolean;
			returnIndex?: boolean;
		} = {},
	) {
		let [s0, s1] = opts.rowSpan ?? [i0, i1];
		let result = {
			bin1_id: [] as number[],
			bin2_id: [] as number[],
			[field]: [] as number[],
		};

		let offsets = (await this.bin1Offsets).slice(s0, s1 + 1);
		let offsetLo = offsets[0];
		let offsetHi = offsets[offsets.length - 1];

		let {
			bin2_id: bin2Extracted,
			[field]: dataExtracted,
		} = await this.pixels
			.select("bin2_id", field)
			.slice(offsetLo, offsetHi);

		// Now, go row by row, filter out unwanted columns, and accumulate the results.
		for (let i = 0; i < (offsets.length - 1); i++) {
			// Shift the global offsets to relative ones.
			let lo = offsets[i] - offsetLo;
			let hi = offsets[i + 1] - offsetLo;

			// Get the j coordinates for this row and filter for the range of j values we want.
			let bin2 = Array.from(bin2Extracted.subarray(lo, hi), Number);
			let data = dataExtracted.subarray(lo, hi);

			for (let j = 0; j < bin2.length; j++) {
				if ((bin2[j] >= j0) && (bin2[j] < j1)) {
					result.bin1_id.push(i + s0); // row
					result.bin2_id.push(bin2[j]); // col
					result[field].push(data[j]); // field
				}
			}
		}

		return result;
	}
}

export default CSRReader;
