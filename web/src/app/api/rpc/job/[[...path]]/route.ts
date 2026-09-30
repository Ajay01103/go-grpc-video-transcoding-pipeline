import { createRpcProxy } from "@/lib/create-rpc-proxy";

// Job-service RPC: pipeline run status, cancel, retry.
const handler = createRpcProxy(
  process.env.JOB_RPC_URL ?? "http://localhost:50080",
  "/api/rpc/job",
);

export { handler as GET, handler as POST };
