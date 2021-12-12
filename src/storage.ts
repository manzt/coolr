import type { Reader, ZipInfo } from "unzipit";
import { unzip } from "unzipit";
import { parse } from "reference-spec-reader";
import type { AbsolutePath, Async, Readable } from "zarrita";
export { default as FetchStore } from "zarrita/storage/fetch";

function getRangeHeader(offset: number, size: number) {
	return {
		Range: `bytes=${offset}-${offset + size - 1}`,
	};
}

function uri2href(url: string | URL) {
	let [protocol, rest] = (typeof url === "string" ? url : url.href).split("://");
	if (protocol === "https" || protocol === "http") {
		return url;
	}
	if (protocol === "gc") {
		return `https://storage.googleapis.com/${rest}`;
	}
	if (protocol === "s3") {
		return `https://s3.amazonaws.com/${rest}`;
	}
	throw Error("Protocol not supported, got: " + JSON.stringify(protocol));
}

type FetchConfig = { url: string | URL; offset?: number; size?: number };

function fetchWithOptionalRange(
	{ url, offset, size }: FetchConfig,
	opts: RequestInit = {},
) {
	if (offset !== undefined && size !== undefined) {
		// merge request opts
		opts = {
			...opts,
			headers: {
				...opts.headers,
				...getRangeHeader(offset, size),
			},
		};
	}
	return fetch(url as string, opts);
}

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
		const req = await fetchWithOptionalRange({ url: this.url, offset, size });
		if (!req.ok) {
			throw new Error(
				`failed http request ${this.url}, status: ${req.status} offset: ${offset} size: ${size}: ${req.statusText}`,
			);
		}
		return new Uint8Array(await req.arrayBuffer());
	}
}

export function stripPrefix<Path extends AbsolutePath>(
	path: Path,
): Path extends AbsolutePath<infer Rest> ? Rest : never {
	return path.slice(1) as any;
}

export class ZipFileStore<R extends Reader> implements Async<Readable> {
	private info: Promise<ZipInfo>;
	constructor(reader: R) {
		this.info = unzip(reader);
	}

	async get(key: AbsolutePath) {
		let entry = (await this.info).entries[stripPrefix(key)];
		if (!entry) return;
		return new Uint8Array(await entry.arrayBuffer());
	}

	async has(key: AbsolutePath) {
		return stripPrefix(key) in (await this.info).entries;
	}

	static fromUrl<Url extends string | URL>(href: Url) {
		return new ZipFileStore(new HTTPRangeReader(href));
	}

	static fromBlob(blob: Blob) {
		return new ZipFileStore(new BlobReader(blob));
	}
}

interface ReferenceStoreOptions {
	target?: string | URL;
}

export class ReferenceStore implements Async<Readable<RequestInit>> {
	private target?: string | URL;

	constructor(private refs: ReturnType<typeof parse>, opts: ReferenceStoreOptions = {}) {
		this.target = opts.target;
	}

	async get(key: AbsolutePath, opts: RequestInit = {}) {
		let ref = this.refs.get(stripPrefix(key));

		if (!ref) return;

		if (typeof ref === "string") {
			let enc = new TextEncoder();
			let ascii = ref.startsWith("base64:") ? atob(ref.slice(7)) : ref;
			return enc.encode(ascii);
		}

		let [urlOrNull, offset, size] = ref;
		let url = urlOrNull ?? this.target;
		if (!url) {
			throw Error(`No url for key ${key}, and no target url provided.`);
		}

		let res = await fetchWithOptionalRange({ url: uri2href(url), offset, size }, opts);

		if (res.status === 200 || res.status === 206) {
			return new Uint8Array(await res.arrayBuffer());
		}

		throw new Error(
			`Request unsuccessful for key ${key}. Response status: ${res.status}.`,
		);
	}

	async has(key: AbsolutePath) {
		return this.refs.has(stripPrefix(key));
	}

	static fromSpec(spec: Record<string, any>, opts: ReferenceStoreOptions = {}) {
		let refs = parse(spec);
		return new ReferenceStore(refs, opts);
	}

	static async fromUrl(refUrl: string | URL, opts: ReferenceStoreOptions = {}) {
		let spec = await fetch(refUrl as string).then((res) => res.json());
		return ReferenceStore.fromSpec(spec, opts);
	}
}
