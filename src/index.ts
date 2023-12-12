export { Cooler, Indexer1D, open } from "./core.js";

import ReferenceStore from "@zarrita/storage/ref";
import { open } from "./core.js";
import type { Cooler } from "./core.js";

// TODO: Better way to find resolutions
function findResolutionGroupsFromReferenceSpec(
	spec: { refs: Record<string, unknown> },
) {
	let grps = new Set<string>();
	for (let key of Object.keys(spec.refs)) {
		let match = key.match(/resolutions\/[0-9]+/);
		if (match) {
			!grps.has(match[0]) && grps.add(match[0]);
		}
	}
	return [...grps];
}

/**
 * Open a multi-res cooler (.mcool) from a kerchunk reference.
 *
 * @param href - The URL of the kerchunk reference.
 */
export async function mcool(href: string | URL): Promise<[string, Cooler][]> {
	let kerchunkReferenceSpec = await fetch(href).then((r) => r.json());
	let store = ReferenceStore.fromSpec(kerchunkReferenceSpec);
	let resolutionGroups = findResolutionGroupsFromReferenceSpec(
		kerchunkReferenceSpec,
	);
	return Promise.all(
		resolutionGroups.map(async (p) => {
			let grp = await open(store, `/${p}`);
			return [p, grp] as const;
		}),
	);
}
