"use client";

import { useParams, useRouter } from "next/navigation";
import { useState } from "react";
import {
  ArrowLeft,
  Check,
  Copy,
  ExternalLink,
  Layers,
  Radio,
  RotateCcw,
  Sparkles,
} from "lucide-react";
import Link from "next/link";

import { VideoPreview } from "@/modules/dashboard/components/video-preview";
import { PipelineProgress } from "@/modules/dashboard/components/pipeline-progress";
import { usePipelineStatus } from "@/modules/dashboard/api/use-video-pipeline";

export default function ReviewPage() {
  const { assetId } = useParams<{ assetId: string }>();
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const { runStatus, steps, playerConfig } = usePipelineStatus(
    assetId ?? null,
    // Always poll — this page is the source of truth regardless of how we got here.
    true,
  );

  const copyToClipboard = (key: string, text: string) => {
    void navigator.clipboard.writeText(text);
    setCopiedKey(key);
    setTimeout(() => {
      setCopiedKey(null);
    }, 2000);
  };

  // If the pipeline fails, stay on this page (show error state).
  const isFailed = runStatus === "FAILED" || runStatus === "CANCELLED";
  const isReady = !!playerConfig;

  return (
    <div className="mx-auto flex max-w-4xl flex-col gap-8 pb-16">
      {/* Header */}
      <div className="flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <Link
            href="/dashboard"
            className="flex items-center gap-1.5 rounded-xl border border-border bg-card px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          >
            <ArrowLeft className="size-3.5" />
            Back to dashboard
          </Link>

          {isReady && (
            <div className="flex items-center gap-2">
              <span className="flex items-center gap-1.5 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-600 dark:text-emerald-400">
                <span className="relative flex size-2">
                  <span className="absolute inline-flex size-full animate-ping rounded-full bg-emerald-400 opacity-75"></span>
                  <span className="relative inline-flex size-2 rounded-full bg-emerald-500"></span>
                </span>
                Ready to stream
              </span>
              <span className="flex items-center gap-1 rounded-full border border-border bg-card px-3 py-1 text-xs font-medium text-muted-foreground">
                <Layers className="size-3" />
                Adaptive HLS
              </span>
            </div>
          )}
        </div>

        <div>
          <h1 className="text-3xl font-bold tracking-tight text-foreground md:text-4xl">
            {isReady ? "Your video is ready" : "Processing video…"}
          </h1>
          <p className="mt-1 text-base text-muted-foreground md:text-lg">
            {isReady
              ? "Your video has been transcoded to adaptive HLS and is ready to play with thumbnail previews and subtitles."
              : "Sit tight — your video is being transcoded to adaptive HLS."}
          </p>
        </div>
      </div>

      {/* Player — shown only after pipeline completes */}
      {isReady ? (
        <div className="flex flex-col gap-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <p className="text-xs font-medium text-muted-foreground">
                Asset ID:
              </p>
              <code className="rounded bg-muted px-2 py-0.5 font-mono text-xs text-foreground">
                {assetId}
              </code>
            </div>

            <Link
              href="/dashboard"
              className="flex items-center gap-1.5 rounded-xl border border-border bg-card px-3.5 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-accent"
            >
              <RotateCcw className="size-3.5" />
              Upload another
            </Link>
          </div>

          {/* Vidstack Video Player */}
          <VideoPreview config={playerConfig} />

          {/* Stream Information Card */}
          <div className="grid gap-4 rounded-2xl border border-border bg-card/60 p-5 shadow-sm backdrop-blur-sm sm:grid-cols-2">
            <div className="flex flex-col gap-1.5">
              <span className="text-xs font-medium text-muted-foreground">
                Playback ID
              </span>
              <div className="flex items-center justify-between gap-2 rounded-xl border border-border bg-background px-3 py-2">
                <span className="truncate font-mono text-xs text-foreground">
                  {playerConfig.playbackId}
                </span>
                <button
                  type="button"
                  onClick={() =>
                    copyToClipboard("playbackId", playerConfig.playbackId)
                  }
                  className="inline-flex shrink-0 items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
                  title="Copy playback ID"
                >
                  {copiedKey === "playbackId" ? (
                    <Check className="size-3.5 text-emerald-500" />
                  ) : (
                    <Copy className="size-3.5" />
                  )}
                </button>
              </div>
            </div>

            <div className="flex flex-col gap-1.5">
              <span className="text-xs font-medium text-muted-foreground">
                HLS Manifest URL (master.m3u8)
              </span>
              <div className="flex items-center justify-between gap-2 rounded-xl border border-border bg-background px-3 py-2">
                <span className="truncate font-mono text-xs text-foreground">
                  {playerConfig.src}
                </span>
                <button
                  type="button"
                  onClick={() => copyToClipboard("hlsSrc", playerConfig.src)}
                  className="inline-flex shrink-0 items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
                  title="Copy HLS URL"
                >
                  {copiedKey === "hlsSrc" ? (
                    <Check className="size-3.5 text-emerald-500" />
                  ) : (
                    <Copy className="size-3.5" />
                  )}
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : isFailed ? (
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3 rounded-2xl border border-destructive/40 bg-destructive/5 p-6">
            <p className="text-base font-semibold text-destructive">
              The pipeline {runStatus === "CANCELLED" ? "was cancelled" : "failed"} for this video.
            </p>
            <p className="text-sm text-muted-foreground">
              Something went wrong during transcoding or one of the pipeline stages.
            </p>
            <Link
              href="/dashboard"
              className="mt-1 w-fit rounded-xl border border-border bg-card px-4 py-2 text-sm font-medium text-foreground transition-colors hover:bg-accent"
            >
              Try uploading another file
            </Link>
          </div>
          {steps.length > 0 && (
            <PipelineProgress steps={steps} runStatus={runStatus} errorMessage={null} />
          )}
        </div>
      ) : (
        /* Progress tracker while processing */
        <PipelineProgress steps={steps} runStatus={runStatus} errorMessage={null} />
      )}
    </div>
  );
}

