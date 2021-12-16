import * as path from "path";
import { defineConfig } from "vite";
import serveStatic from "serve-static";

const resolveDataDir = (fp) => {
	if (fp[0] === "~") {
		return path.join(process.env.HOME, fp.slice(1));
	}
	return path.resolve(__dirname, fp);
};

/**
 * Static file server. Adds `@data/` route during development.
 *
 * @returns {import('vite').Plugin} 
 */
const serveData = (dir) => {
	dir = resolveDataDir(dir);
	const serve = serveStatic(dir, { dotfiles: "allow" });
	return {
		name: "serve-data-dir",
		apply: "serve",
		configureServer(server) {
			server.middlewares.use((req, res, next) => {
				if (/^\/@data\//.test(req.url)) {
					req.url = req.url.replace("/@data/", "");
					serve(req, res, next);
				} else {
					next();
				}
			});
		},
	};
};

export default defineConfig({ 
	resolve: {
		alias: { "@manzt/coolr": "./src/index.ts" },
	},
	plugins: [
		serveData("./data"),
	] 
});
