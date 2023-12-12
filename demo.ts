import * as cooler from "./src/index.js";

let base = new URL("/@data/", import.meta.url);
base.pathname = base.pathname.replace("/@fs", "") + "/";
console.log(base.href);

function scaleFn(max: number, min: number = 0) {
	let a = 0;
	let b = 255;
	return (x: number) => (b - a) * (x - min) / (max - min) + a;
}

let resolutions = await cooler.mcool(
	new URL("test.mcool.remote.json", base).href,
);

// get highest resolution
let [name, c] = resolutions[0];
let size = 1024;

console.log(`${name}, region: [0:${size}, 0:${size}]`);
(window as any).c = c;

// read region as dense array
let { data, shape: [height, width] } = await c.matrix.slice(0, size, 0, size);

// scale values to 0-255 & create RGBA image

let min = Infinity;
let max = -Infinity;
for (let i = 0; i < data.length; i++) {
	if (data[i] > max) max = data[i];
	if (data[i] < min) min = data[i];
}

let scale = scaleFn(max, min);
let rgba = new Uint8ClampedArray(data.length * 4);
let offset = 0;

for (let i = 0; i < data.length; i++) {
	let value = Math.floor(scale(data[i]));
	if (value >= 0) {
		rgba[offset + 0] = value;
		rgba[offset + 1] = 0;
		rgba[offset + 2] = 0;
	} else {
		rgba[offset] = 255;
		rgba[offset + 1] = 255;
		rgba[offset + 2] = 255;
	}
	rgba[offset + 3] = 255;
	offset += 4;
}

let img = new ImageData(rgba, width);
let canvas = document.querySelector("canvas")!;
let dpi = devicePixelRatio;
canvas.width = width * dpi;
canvas.height = height * dpi;
canvas.style.width = width + "px";
var ctx = canvas.getContext("2d")!;
ctx.putImageData(img, 0, 0);
