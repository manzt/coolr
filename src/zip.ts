import { unzip, HTTPRangeReader, ZipEntry } from 'unzipit';

let removePrefix = (key: string) => key[0] === '/' ? key.slice(1) : key;

export class ZipFileStore {
	constructor(public refs: Record<string, ZipEntry>) {}

	async get(key: string) {
		let entry = this.refs[removePrefix(key)];
		if (!entry) return;
		return new Uint8Array(await entry.arrayBuffer());
	}

	async has(key: string) {
		return removePrefix(key) in this.refs;
	}

	static async fromUrl(href: string) {
		let reader = new HTTPRangeReader(href);
		let {entries} = await unzip(reader as any);
		return new ZipFileStore(entries);
	}

	static async fromBlob(blob: Blob) {
		let {entries} = await unzip(blob);
		return new ZipFileStore(entries);
	}
}
