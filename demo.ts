import * as cooler from "@manzt/coolr";

import _FetchStore from "zarrita/storage/fetch";
import ReferenceStore from "zarrita/storage/ref";
import ZipFileStore from "zarrita/storage/zip";

let input = document.querySelector("input[type=file]")!;
input.addEventListener("change", async (e: Event) => {
	let file = (e.target as HTMLInputElement).files![0];
	let c = await cooler.open(ZipFileStore.fromBlob(file));
	console.log(c);
});

// configured only for dev in vite.config.js
let base = new URL("http://localhost:3000/@data/");
let c = await cooler.open(
	await ReferenceStore.fromUrl(new URL("test.mcool.remote.json", base)),
	"/resolutions/1000",
);

// add to window so can access in browser console
(window as any).c = c;
