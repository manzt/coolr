import { defineConfig } from "vite";
import serveStatic from "serve-static";

export default defineConfig({ 
	resolve: {
		alias: { "@manzt/coolr": "./src/index.ts" },
	},
	plugins: [
		{
			name: "server-data-dir",
			apply: "serve",
			configureServer(server) {
				server.middlewares.use(
					"/@data",
					serveStatic("./data", { dotfiles: "allow" }),
				)
			},
		}
	]
});
