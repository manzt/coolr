import type { Reader, ZipInfo } from "unzipit";
import { unzip } from "unzipit";
import ReadOnlyStore from "zarrita/storage/readonly";

class BlobReader implements Reader {
	constructor(public blob: Blob) {}
	async getLength() {
		return this.blob.size;
	}
	async read(offset: number, length: number) {
		const blob = this.blob.slice(offset, offset + length);
		return new Uint8Array(await blob.arrayBuffer());
	}
}

class HTTPRangeReader<Url extends string | URL> implements Reader {
	private length?: number;
	constructor(public url: Url) {}

	async getLength() {
		if (this.length === undefined) {
			const req = await fetch(this.url as string, { method: "HEAD" });
			if (!req.ok) {
				throw new Error(
					`failed http request ${this.url}, status: ${req.status}: ${req.statusText}`,
				);
			}
			this.length = parseInt(req.headers.get("content-length")!);
			if (Number.isNaN(this.length)) {
				throw Error("could not get length");
			}
		}
		return this.length;
	}

	async read(offset: number, size: number) {
		if (size === 0) {
			return new Uint8Array(0);
		}
		const req = await fetch(this.url as string, {
			headers: {
				Range: `bytes=${offset}-${offset + size - 1}`,
			},
		});
		if (!req.ok) {
			throw new Error(
				`failed http request ${this.url}, status: ${req.status} offset: ${offset} size: ${size}: ${req.statusText}`,
			);
		}
		return new Uint8Array(await req.arrayBuffer());
	}
}

function removePrefix<Key extends string>(
	key: Key,
): Key extends `/${infer Rest}` ? Rest : Key {
	return key[0] === "/" ? key.slice(1) : key as any;
}

export class ZipFileStore<R extends Reader> extends ReadOnlyStore {
	private info: Promise<ZipInfo>;
	constructor(reader: R) {
		super();
		this.info = unzip(reader);
	}

	async get(key: string) {
		let entry = (await this.info).entries[removePrefix(key)];
		if (!entry) return;
		return new Uint8Array(await entry.arrayBuffer());
	}

	async has(key: string) {
		return removePrefix(key) in (await this.info).entries;
	}

	static fromUrl<Url extends string | URL>(href: Url) {
		return new ZipFileStore(new HTTPRangeReader(href));
	}

	static fromBlob(blob: Blob) {
		return new ZipFileStore(new BlobReader(blob));
	}
}
