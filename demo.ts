import * as cooler from "@manzt/coolr";
import type { Readable, Async } from "zarrita";

async function run<Store extends Async<Readable>>(store: Store, name: string, path?: `/${string}`) {
	let c = await cooler.Cooler.open(store, path);
	console.time(name);
	let count = await c.pixels.select("count").slice(10);
	let pixels = await c.pixels.slice(10);
	let chroms = await c.chroms();
	console.timeEnd(name);
	console.log({ pixels, count, chroms });
	return c;
}

let [
	{ default: _FetchStore },
	{ default: ReferenceStore },
	{ default: ZipFileStore },
] = await Promise.all([
	import("zarrita/storage/fetch"),
	import("zarrita/storage/ref"),
	import("zarrita/storage/zip"),
]);

// configured only for dev in vite.config.js
let base = new URL("http://localhost:3000/@data/");
let input = document.querySelector("input[type=file]")!;

input.addEventListener("change", async (e: Event) => {
	let file = (e.target as HTMLInputElement).files![0];
	let store = ZipFileStore.fromBlob(file);
	run(store, "File");
});

let c = await run(
	await ReferenceStore.fromUrl(new URL("test.mcool.remote.json", base)),
	"hdf5",
	"/resolutions/10000",
);

console.time("first");
console.log(await c.extent("chr17:82,200,000-83,200,000"));
console.timeEnd("first");

console.time("second");
console.log(await c.extent("chr17:82,200,000-83,200,000"));
console.timeEnd("second");
console.log(c);
