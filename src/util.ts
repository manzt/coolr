import type { AbsolutePath, Async, Readable } from "zarrita";
import type { Codec } from "numcodecs";

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
