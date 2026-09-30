"use client";

import { create } from "@bufbuild/protobuf";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import {
  CompleteUploadRequestSchema,
  CreateAssetRequestSchema,
  CreateUploadURLRequestSchema,
  GetAssetRequestSchema,
} from "@/gen/pb/assets/assets_pb";
import { GetPipelineStatusRequestSchema } from "@/gen/pb/job/job_pb";
import { GetPlayerConfigRequestSchema } from "@/gen/pb/playback/playback_pb";
import { assetRpcClient, jobRpcClient, playbackRpcClient } from "@/lib/rpc";

/** Lifecycle states of one upload+transcode flow. */
export type PipelinePhase =
  | "idle"
  | "creating-asset"
  | "uploading"
  | "starting-pipeline"
  | "processing"
  | "ready"
  | "errored";

export interface PipelineStepView {
  step: string;
  status: string;
}

export interface VideoPipelineState {
  phase: PipelinePhase
  assetId: string | null
  uploadProgress: number // 0..100
  runStatus: string | null
  steps: PipelineStepView[]
  errorMessage: string | null
  playerConfig: PlayerConfigView | null
  fileName: string | null
}

export interface PlayerConfigView {
  src: string;
  poster: string;
  storyboardSrc: string;
  subtitles: { url: string; lang: string; label: string; auto: boolean }[];
  assetId: string;
  playbackId: string;
}

const TERMINAL_RUN_STATUSES = new Set(["COMPLETED", "FAILED", "CANCELLED"]);

interface UploadState {
  phase: PipelinePhase;
  uploadProgress: number;
  errorMessage: string | null;
}

/**
 * Drives the full flow: CreateAsset → PUT to presigned URL → CompleteUpload →
 * poll GetPipelineStatus until the run finishes → GetPlayerConfig for Vidstack.
 */
export function useVideoPipeline() {
  const queryClient = useQueryClient();
  const router = useRouter();
  const [upload, setUpload] = useState<UploadState>({
    phase: "idle",
    uploadProgress: 0,
    errorMessage: null,
  });
  const [assetId, setAssetId] = useState<string | null>(null);
  const [fileName, setFileName] = useState<string | null>(null);
  const [playerConfig, setPlayerConfig] = useState<PlayerConfigView | null>(null);
  const runIdRef = useRef<string | null>(null);
  const fileRef = useRef<File | null>(null);

  const reset = useCallback(() => {
    setUpload({ phase: "idle", uploadProgress: 0, errorMessage: null });
    setAssetId(null);
    setFileName(null);
    setPlayerConfig(null);
    runIdRef.current = null;
    fileRef.current = null;
  }, []);

  const start = useCallback(
    async (file: File) => {
      fileRef.current = file;
      setFileName(file.name);
      setPlayerConfig(null);
      runIdRef.current = null;
      setUpload({ phase: "creating-asset", uploadProgress: 0, errorMessage: null });

      try {
        // 1. Create the asset record.
        // org_id is intentionally not sent — the asset service applies a
        // default dev org until multi-org support is integrated.
        const created = await assetRpcClient.createAsset(
          create(CreateAssetRequestSchema, {
            title: file.name,
          }),
        );
        const newAssetId = created.asset?.assetId ?? "";
        if (!newAssetId) throw new Error("asset service returned no asset id");
        setAssetId(newAssetId);

        // 2. Presigned PUT directly to RustFS.
        setUpload((state) => ({ ...state, phase: "uploading" }));
        const uploadUrlResponse = await assetRpcClient.createUploadURL(
          create(CreateUploadURLRequestSchema, { assetId: newAssetId }),
        );
        const uploadUrl = uploadUrlResponse.uploadUrl;
        if (!uploadUrl) throw new Error("asset service returned no upload URL");

        const putResponse = await fetch(uploadUrl, {
          method: "PUT",
          body: file,
          headers: { "Content-Type": "application/octet-stream" },
        });
        if (!putResponse.ok) {
          throw new Error(`storage rejected the upload (${putResponse.status})`);
        }
        setUpload((state) => ({ ...state, uploadProgress: 100 }));

        // 3. CompleteUpload flips the asset to PROCESSING and publishes
        //    assets.upload.completed — the pipeline run starts here.
        setUpload((state) => ({ ...state, phase: "starting-pipeline" }));
        const sourceUri = `s3://${uploadUrlResponse.bucket}/${uploadUrlResponse.objectKey}`;
        await assetRpcClient.completeUpload(
          create(CompleteUploadRequestSchema, {
            assetId: newAssetId,
            sourceUri,
          }),
        );
        setUpload((state) => ({ ...state, phase: "processing" }));
        void queryClient.invalidateQueries({ queryKey: ["pipeline-status", newAssetId] });
        // Redirect to the dedicated review page so the player appears
        // automatically once transcoding finishes — no matter how long it takes.
        router.push(`/review/${newAssetId}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : "upload failed";
        setUpload((state) => ({ ...state, phase: "errored", errorMessage: message }));
      }
    },
    [queryClient, router],
  );

  return { upload, assetId, fileName, playerConfig, start, reset };
}

/**
 * Polls GetPipelineStatus every 2s while the run is in flight, then fetches
 * the player config once the run completes.
 */
export function usePipelineStatus(assetId: string | null, enabled: boolean) {
  const queryClient = useQueryClient();
  const [playerConfig, setPlayerConfig] = useState<PlayerConfigView | null>(null);

  const query = useQuery({
    queryKey: ["pipeline-status", assetId],
    enabled: enabled && assetId !== null,
    refetchInterval: (query) => {
      const runStatus = query.state.data?.run?.status;
      return runStatus && TERMINAL_RUN_STATUSES.has(runStatus) ? false : 2000;
    },
    refetchIntervalInBackground: true,
    staleTime: 0,
    retry: 1,
    queryFn: async () => {
      const response = await jobRpcClient.getPipelineStatus(
        create(GetPipelineStatusRequestSchema, { assetId: assetId! }),
      );
      return response;
    },
  });

  const runStatus = query.data?.run?.status ?? null;

  useEffect(() => {
    if (runStatus !== "COMPLETED" || playerConfig || !assetId) return;
    const playbackId = query.data?.run?.playbackId ?? "";
    if (!playbackId) return;

    let cancelled = false;
    void (async () => {
      try {
        // No cdnUrl: the playback service emits direct public RustFS URLs
        // (bucket policy grants anonymous GET on assets/*, CORS enabled).
        const config = await playbackRpcClient.getPlayerConfig(
          create(GetPlayerConfigRequestSchema, { playbackId }),
        );
        if (!cancelled) {
          setPlayerConfig({
            src: config.src,
            poster: config.poster,
            storyboardSrc: config.storyboardSrc,
            subtitles: config.subtitles.map((track) => ({
              url: track.url,
              lang: track.lang,
              label: track.label,
              auto: track.auto,
            })),
            assetId: config.assetId,
            playbackId: config.playbackId,
          });
        }
      } catch (error) {
        console.error("[dashboard] fetch player config failed", error);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [runStatus, assetId, query.data?.run?.playbackId, playerConfig, queryClient]);

  return {
    runStatus,
    steps: (query.data?.steps ?? []).map((step) => ({
      step: step.step,
      status: step.status,
    })),
    playbackId: query.data?.run?.playbackId ?? "",
    playerConfig,
    isPolling: query.isFetching,
  };
}
