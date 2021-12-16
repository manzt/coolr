import type { Cooler } from "./index";
import type { Int32, Int64, TypedArray } from "zarrita/dtypes";

class CSRReader<Field extends Cooler["pixels"]["fields"][number]> {
	constructor(
		public pixels: Cooler["pixels"],
		public indexes: Cooler["indexes"],
		public field: Field,
		public maxChunk: bigint,
	) {}

	/** Retrieve pixel table row IDs corresponding to query rectangle. */
	async indexCol(i0: number, i1: number, j0: number, j1: number) {
		let edges = await this.#edges(i0, i1);
		let index: number[] = [];
		for (let i = 0; i < edges.length - 1; i++) {
			let lo1 = edges[i];
			let hi1 = edges[i + 1];
			if (hi1 - lo1 > 0n) {
				let bin2 = await this.#bin2(Number(lo1), Number(hi1));
				let nlo1 = Number(lo1);
				let bj0 = BigInt(j0);
				let bj1 = BigInt(j1);
				for (let i = 0; i < bin2.length; i++) {
					if (bin2[i] >= bj0 && bin2[i] < bj1) {
						index.push(i + nlo1);
					}
				}
			}
		}
		return index;
	}

	#edges(i0: number, i1: number) {
		return this.indexes
			.select("bin1_offset")
			.slice(i0, i1 + 1);
	}

	#bin2(lo: number, hi: number) {
		return this.pixels
			.select("bin2_id")
			.slice(lo, hi);
	}

	#data(lo: number, hi: number) {
		return this.pixels
			.select(this.field)
			.slice(lo, hi) as Promise<TypedArray<Int32 | Int64>>;
	}

	/** Retrieve sparse matrix data inside a query rectangle. */
	async query(i0: number, i1: number, j0: number, j1: number) {
		let i: number[] = [];
		let j: number[] = [];
		let v: number[] = [];

		if ((i1 - i0 > 0) || (j1 - j0 > 0)) {
			let edges = await this.#edges(i0, i1);
			let p0 = edges[0];
			let p1 = edges[edges.length - 1];

			if ((p1 - p0) < BigInt(this.maxChunk)) {
				let allBin2 = await this.#bin2(Number(p0), Number(p1));
				let allData = await this.#data(Number(p0), Number(p1));

				for (let _i = 0; _i < edges.length - 1; _i++) {
					let rowId = i0 + _i;
					let lo = Number(edges[_i] - p0);
					let hi = Number(edges[_i + 1] - p0);
					let bin2 = allBin2.subarray(lo, hi);
					let data = allData.subarray(lo, hi);

					for (let _j = 0; _j < bin2.length; _j++) {
						if (bin2[_j] >= BigInt(j0) && bin2[_j] < BigInt(j1)) {
							i.push(rowId);
							j.push(Number(bin2[_j]));
							v.push(Number(data[_j]));
						}
					}
				}
			} else {
				for (let _i = 0; _i < edges.length - 1; _i++) {
					let rowId = i0 + _i;
					let lo = Number(edges[_i] - p0);
					let hi = Number(edges[_i + 1] - p0);

					let [bin2, data] = await Promise.all([this.#bin2(lo, hi), this.#data(lo, hi)]);

					for (let _j = 0; _j < bin2.length; _j++) {
						if (bin2[_j] >= BigInt(j0) && bin2[_j] < BigInt(j1)) {
							i.push(rowId);
							j.push(Number(bin2[_j]));
							v.push(Number(data[_j]));
						}
					}
				}
			}
		}

		return { i, j, v };
	}
}

export default CSRReader;
