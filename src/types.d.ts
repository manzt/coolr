import type { Async, DataType, Readable, TypedArray } from "zarrita";
import type * as zarr from "zarrita/v2";
import type { ByteStr, Float64, Int32, Int64 } from "zarrita/dtypes";

export interface CoolerInfo {
	// Required attributes
	"format": string;
	"format-version": number;
	"bin-type": "fixed" | "variable";
	"bin-size": number | null;
	"storage-mode": "symmetric-upper" | "square";
	// Reserved but optional fields
	"assembly"?: string;
	"generated-by"?: string;
	"creation-date"?: Date;
	"metadata"?: Record<string, any>;
}

export type Dataset<
	Store extends Async<Readable>,
	Nodes extends Record<string, DataType>,
> = {
	[Key in keyof Nodes]: zarr.Array<Nodes[Key], Store>;
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
		count: Int32 | Float64;
	}>;
};

type Table<T extends Dataset<any, any>, K extends keyof T> = {
	[Key in K]: T[Key] extends zarr.Array<infer D, any> ? TypedArray<D> : never;
};

export type DataSlice<T extends Dataset<any, any>, K extends keyof T> = IsUnion<K> extends
	true ? Table<T, K>
	: Table<T, K>[K]; // extract TypedArray if only one key

export type Region = [chrom: string, start: number | null, end: number | null];
export type NormedRegion = [chrom: string, start: number, end: number];
export type Extent = [number, number];

type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends
	((k: infer I) => void) ? I : never;

type IsUnion<T> = [T] extends [UnionToIntersection<T>] ? false : true;
