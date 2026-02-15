import { resolve } from "path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/ice/",
  plugins: [react()],
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),
        headlines: resolve(__dirname, "headlines.html"),
        protests: resolve(__dirname, "protests.html"),
        stats: resolve(__dirname, "stats.html"),
        charts: resolve(__dirname, "charts.html"),
        deaths: resolve(__dirname, "deaths.html"),
      },
    },
  },
  server: {
    port: 3000,
    host: "0.0.0.0",
  },
});
