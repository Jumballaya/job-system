import { createDemoSystem } from "./system.js";

if (process.env.JOB_BACKEND === "memory") throw new Error("The standalone demo worker requires Redis");
const { jobs, label } = createDemoSystem();
const stop = () => {
  void jobs.close().catch((error: unknown) => {
    console.error("Worker shutdown failed:", error);
    process.exitCode = 1;
  });
};
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
console.log(`Worker initialized: ${label}`);
