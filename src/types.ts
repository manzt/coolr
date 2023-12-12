import type * as zarr from "zarrita";
import type { Readable } from "@zarrita/storage";

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

export function isCoolerInfo(info: any): info is CoolerInfo {
	return (
		typeof info === "object" &&
		typeof info["format"] === "string" &&
		typeof info["format-version"] === "number" &&
		typeof info["bin-type"] === "string" &&
		(info["bin-size"] === null || typeof info["bin-size"] === "number") &&
		typeof info["storage-mode"] === "string"
	);
}

export type Dataset<
	Store extends Readable,
	Nodes extends Record<string, zarr.DataType>,
> = {
	[Key in keyof Nodes]: zarr.Array<Nodes[Key], Store>;
};

export type CoolerDataset<Store extends Readable = Readable> = {
	bins: Dataset<Store, {
		chrom: zarr.Int32;
		start: zarr.Int32;
		end: zarr.Int32;
		weight: zarr.Float64;
	}>;
	chroms: Dataset<Store, {
		name: zarr.ByteStr;
		length: zarr.Int32;
	}>;
	indexes: Dataset<Store, {
		bin1_offset: zarr.Int64;
		chrom_offset: zarr.Int64;
	}>;
	pixels: Dataset<Store, {
		bin1_id: zarr.Int64;
		bin2_id: zarr.Int64;
		count: zarr.Int32 | zarr.Float64;
	}>;
};

type Table<T extends Dataset<any, any>, K extends keyof T> = {
	[Key in K]: T[Key] extends zarr.Array<infer D, any> ? zarr.TypedArray<D>
		: never;
};

export type DataSlice<T extends Dataset<any, any>, K extends keyof T> =
	IsUnion<K> extends true ? Table<T, K>
		: Table<T, K>[K]; // extract TypedArray if only one key

export type Region = [chrom: string, start: number | null, end: number | null];
export type NormedRegion = [chrom: string, start: number, end: number];
export type Extent = [number, number];

type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends
	((k: infer I) => void) ? I : never;

type IsUnion<T> = [T] extends [UnionToIntersection<T>] ? false : true;
