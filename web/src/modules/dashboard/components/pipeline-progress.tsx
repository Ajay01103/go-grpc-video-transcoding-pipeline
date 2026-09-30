"use client";

import { Check, Loader2, X } from "lucide-react";

import type { PipelineStepView } from "../api/use-video-pipeline";

const STEP_LABELS: Record<string, string> = {
  probe: "Analyzing source",
  transcode: "Transcoding to HLS",
  thumbnail: "Generating poster",
  storyboard: "Building scrub preview",
  subtitle: "Generating captions",
  package: "Packaging",
  publish: "Publishing",
};

function StepIcon({ status }: { status: string }) {
  switch (status) {
    case "DONE":
      return <Check className="size-4 text-emerald-500" />;
    case "RUNNING":
      return <Loader2 className="size-4 animate-spin text-info" />;
    case "FAILED":
      return <X className="size-4 text-destructive" />;
    default:
      return <span className="block size-2 rounded-full bg-muted-foreground/40" />;
  }
}

export function PipelineProgress({
  steps,
  runStatus,
  errorMessage,
}: {
  steps: PipelineStepView[];
  runStatus: string | null;
  errorMessage: string | null;
}) {
  if (runStatus === "COMPLETED") return null;

  return (
    <div className="flex flex-col gap-2 rounded-2xl border border-border bg-card p-5">
      <div className="flex items-center justify-between">
        <p className="text-sm font-semibold text-foreground">Processing your video</p>
        <span className="text-xs text-muted-foreground">
          {runStatus === "FAILED"
            ? "Failed"
            : runStatus === "CANCELLED"
              ? "Cancelled"
              : "In progress…"}
        </span>
      </div>
      {errorMessage ? (
        <p className="text-sm text-destructive">{errorMessage}</p>
      ) : null}
      <ul className="flex flex-col gap-1.5">
        {steps.map((step) => (
          <li key={step.step} className="flex items-center gap-2 text-sm">
            <StepIcon status={step.status} />
            <span
              className={
                step.status === "DONE"
                  ? "text-muted-foreground"
                  : step.status === "FAILED"
                    ? "text-destructive"
                    : "text-foreground"
              }
            >
              {STEP_LABELS[step.step] ?? step.step}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
