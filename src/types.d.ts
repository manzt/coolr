import type { Async, DataType, Readable, TypedArray } from "zarrita";
import type { Array as ZarrArray } from "zarrita/v2";
import type { ByteStr, Float64, Int32, Int64 } from "zarrita/dtypes";

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
	"creation-date"?: Date;
	"metadata"?: Record<string, any>;
}

export type Dataset<
	Store extends Async<Readable>,
	Nodes extends Record<string, DataType>,
> = {
	[Key in keyof Nodes]: ZarrArray<Nodes[Key], Store>;
};

export type CoolerDataset<Store extends Async<Readable> = Async<Readable>> = {
	bins: Dataset<Store, {
		chrom: Int32;
		start: Int32;
		end: Int32;
		weight: Float64;
	}>;
	chroms: Dataset<Store, {
		name: ByteStr<64>;
		length: Int32;
	}>;
	indexes: Dataset<Store, {
		bin1_offset: Int64;
		chrom_offset: Int64;
	}>;
	pixels: Dataset<Store, {
		bin1_id: Int64;
		bin2_id: Int64;
		count: Int32;
	}>;
};

export type DataSlice<T extends Dataset<any, any>, K extends keyof T> = {
	[Key in K]: T[Key] extends ZarrArray<infer D, any> ? TypedArray<D> : never;
};

export type Region = [chrom: string, start: number | null, end: number | null];
export type NormedRegion = [chrom: string, start: number, end: number];
export type Extent = [number, number];
