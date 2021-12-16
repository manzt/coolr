import { Cooler } from "@manzt/coolr";

import _FetchStore from "zarrita/storage/fetch";
import ReferenceStore from "zarrita/storage/ref";
import ZipFileStore from "zarrita/storage/zip";

let input = document.querySelector("input[type=file]")!;
input.addEventListener("change", async (e: Event) => {
	let file = (e.target as HTMLInputElement).files![0];
	let cooler = await Cooler.open(ZipFileStore.fromBlob(file));
	console.log(cooler);
});

// configured only for dev in vite.config.js
let base = new URL("http://localhost:3000/@data/");
let cooler = await Cooler.open(
	await ReferenceStore.fromUrl(new URL("test.mcool.remote.json", base)),
	"/resolutions/1000",
);

// add to window so can access in browser console
(window as any).c = cooler;
