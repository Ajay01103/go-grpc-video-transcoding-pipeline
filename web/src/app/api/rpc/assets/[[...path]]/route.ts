import { createRpcProxy } from "@/lib/create-rpc-proxy";

// Asset-service RPC: create/list assets, presigned upload URLs, complete upload.
const handler = createRpcProxy(
  process.env.ASSET_RPC_URL ?? "http://localhost:50060",
  "/api/rpc/assets",
);

export { handler as GET, handler as POST };
