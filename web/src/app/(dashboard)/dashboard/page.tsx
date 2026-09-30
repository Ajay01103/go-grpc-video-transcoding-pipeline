"use client";

import { RotateCcw } from "lucide-react";

import { PipelineProgress } from "@/modules/dashboard/components/pipeline-progress";
import { UploadCard } from "@/modules/dashboard/components/upload-card";
import { VideoPreview } from "@/modules/dashboard/components/video-preview";
import { usePipelineStatus, useVideoPipeline } from "@/modules/dashboard/api/use-video-pipeline";

export default function DashboardPage() {
  const { upload, assetId, fileName, playerConfig, start, reset } = useVideoPipeline();
  const { runStatus, steps, playerConfig: polledConfig } = usePipelineStatus(
    assetId,
    upload.phase === "processing",
  );

  const activeConfig = playerConfig ?? polledConfig;
  const isErrored = upload.phase === "errored" || runStatus === "FAILED";

  return (
    <div className="mx-auto flex max-w-3xl flex-col gap-8">
      <div className="flex flex-col gap-2">
        <h1 className="text-4xl font-bold tracking-tight text-foreground">
          Upload a video
        </h1>
        <p className="text-lg text-muted-foreground">
          Your upload is transcoded to adaptive HLS and previewed here when it&apos;s ready
        </p>
      </div>

      {activeConfig ? (
        <div className="flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium text-foreground">
              {fileName ?? "Your video"} — ready to play
            </p>
            <button
              type="button"
              onClick={reset}
              className="flex items-center gap-1.5 rounded-xl border border-border bg-card px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent"
            >
              <RotateCcw className="size-3.5" />
              Upload another
            </button>
          </div>
          <VideoPreview config={activeConfig} />
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          <p className="text-sm text-muted-foreground">
            {upload.phase === "idle"
              ? "Pick a file to upload"
              : upload.phase === "uploading"
                ? `Uploading${fileName ? ` ${fileName}` : ""}…`
                : upload.phase === "processing"
                  ? "Pipeline running — this can take a minute or two"
                  : null}
          </p>
          {upload.phase === "processing" || upload.phase === "starting-pipeline" ? (
            <PipelineProgress
              steps={steps}
              runStatus={runStatus}
              errorMessage={upload.errorMessage}
            />
          ) : (
            <UploadCard
              onStartUpload={start}
              phase={upload.phase}
              disabled={isErrored && upload.phase === "errored" ? false : false}
            />
          )}
          {upload.phase === "uploading" || upload.phase === "creating-asset" ? (
            <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-primary transition-all"
                style={{ width: `${Math.max(upload.uploadProgress, 8)}%` }}
              />
            </div>
          ) : null}
          {isErrored ? (
            <div className="flex flex-col gap-2 rounded-2xl border border-destructive/40 bg-destructive/5 p-5">
              <p className="text-sm font-medium text-destructive">
                {upload.errorMessage ?? "The pipeline failed to process this video."}
              </p>
              <button
                type="button"
                onClick={reset}
                className="w-fit rounded-xl border border-border bg-card px-4 py-2 text-sm font-medium text-foreground transition-colors hover:bg-accent"
              >
                Try another file
              </button>
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}
