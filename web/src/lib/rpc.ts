"use client";

import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";

import { AssetService } from "@/gen/pb/assets/assets_pb";
import { PlaybackService } from "@/gen/pb/playback/playback_pb";
import { JobService } from "@/gen/pb/job/job_pb";

// Same-origin proxy paths — browser calls Next.js, Next.js attaches Bearer
// from the HttpOnly cookie and forwards to the real Go service.
// No NEXT_PUBLIC_*_RPC_URL, no credentials:include, no token in JS.
const ASSET_BASE_URL = "/api/rpc/assets";
const PLAYBACK_BASE_URL = "/api/rpc/playback";
const JOB_BASE_URL = "/api/rpc/job";

function createTransport(baseUrl: string) {
  // Go services use connectrpc with h2c — they accept grpc-web but not the
  // Connect protocol's application/connect+proto content type.
  // gRPC-Web works over HTTP/1.1 (what the browser → Next.js leg uses) and
  // the proxy forwards it unchanged to the Go service over HTTP/1.1 h2c.
  return createGrpcWebTransport({
    baseUrl,
    useBinaryFormat: true,
  });
}

export const assetRpcClient = createClient(AssetService, createTransport(ASSET_BASE_URL));
export const playbackRpcClient = createClient(PlaybackService, createTransport(PLAYBACK_BASE_URL));
export const jobRpcClient = createClient(JobService, createTransport(JOB_BASE_URL));
