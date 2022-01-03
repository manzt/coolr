export { Cooler, Indexer1D, open } from "./core";

import { open } from "./core";

export async function mcool(href: string) {
	let { default: ReferenceStore } = await import("zarrita/storage/ref");
	let store = await ReferenceStore.fromUrl(href);
	// TODO: Make refs public on store?
	let { refs } = store as any as { refs: Map<string, unknown> };
	let grps = new Set<string>();
	// TODO: better way to find resolutions
	for (let key of refs.keys()) {
		let match = key.match(/resolutions\/[0-9]+/);
		if (match) {
			!grps.has(match[0]) && grps.add(match[0]);
		}
	}
	return Promise.all([...grps].map(async (p) => {
		let grp = await open(store, `/${p}`);
		return [p, grp] as const;
	}));
}
