import { plugin } from "./dist/eslint-plugin.js";

export default [
  {
    files: ["dist/**/*.js"],
    plugins: { eventfabric: plugin },
    rules: {
      "eventfabric/no-default-export": "error",
      "no-console": "error",
    },
  },
];
