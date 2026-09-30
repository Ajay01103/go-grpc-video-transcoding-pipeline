import { createRpcProxy } from "@/lib/create-rpc-proxy";

// Playback-service RPC: player config with signed media URLs, tokens.
const handler = createRpcProxy(
  process.env.PLAYBACK_RPC_URL ?? "http://localhost:50070",
  "/api/rpc/playback",
);

export { handler as GET, handler as POST };
