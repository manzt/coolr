import type { Async, DataType, Readable, TypedArray, ZarrArray } from "zarrita";
import type { ByteStr, Float64, Int32, Int64 } from "zarrita/dtypes";

type Dataset<
	Store extends Readable | Async<Readable>,
	Nodes extends Record<string, unknown>,
> = {
	[Key in keyof Nodes]: Nodes[Key] extends DataType
		? ZarrArray<DataType & Nodes[Key], Store>
		: Nodes[Key] extends Record<string, unknown> ? Dataset<Store, Nodes[Key]>
		: never;
};

export interface CoolerInfo {
	"format": string;
	"format-version": number;
	"bin-type": "fixed" | "variable";
	"bin-size": number;
	"storage-mode": "symmetric-upper" | "square";
	"nbins": number;
	"chroms": number;
	"nnz": number;
	"assembly": string | null;
	"generated-by"?: string;
	"creation-date"?: string;
	"metadata"?: string;
}

export type CoolerDataset<Store extends Readable | Async<Readable> = Async<Readable>> =
	Dataset<
		Store,
		{
			bins: {
				chrom: Int32;
				start: Int32;
				end: Int32;
				weight: Float64;
			};
			chroms: {
				name: ByteStr<64>;
				length: Int32;
			};
			indexes: {
				bin1_offset: Int64;
				chrom_offset: Int64;
			};
			pixels: {
				bin1_id: Int64;
				bin2_id: Int64;
				count: Int32;
			};
		}
	>;

export type SliceData<
	Group extends keyof CoolerDataset,
	Cols extends keyof CoolerDataset[Group],
> = {
	[Key in Cols]: CoolerDataset[Group][Key] extends ZarrArray<infer D, any> ? TypedArray<D>
		: never;
};
