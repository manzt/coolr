import type { ZarrArray } from "zarrita";
import type { AsyncStore, DataType, TypedArray } from "zarrita/types";

type Int32 = ">i4" | "<i4";
type Int64 = ">i8" | "<i8";
type Float64 = ">f8" | "<f8";
type ByteStr = "|S64";

type Dataset<
	Store extends AsyncStore,
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

export type CoolerDataset<Store extends AsyncStore = AsyncStore> = Dataset<
	Store,
	{
		bins: {
			chrom: Int32;
			start: Int32;
			end: Int32;
			weight: Float64;
		};
		chroms: {
			name: ByteStr;
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
	[Key in Cols]: CoolerDataset[Group][Key] extends ZarrArray<infer D, any>
		? TypedArray<D>
		: never;
};
