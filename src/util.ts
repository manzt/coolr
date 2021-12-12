import type { Async, Readable, AbsolutePath } from "zarrita";

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
