"use client";

import { useCallback, useRef, useState, type DragEvent } from "react";
import { Loader2, UploadCloud } from "lucide-react";

import type { PipelinePhase } from "../api/use-video-pipeline";

interface UploadCardProps {
  onStartUpload: (file: File) => void;
  phase: PipelinePhase;
  disabled?: boolean;
}

const BUSY_PHASES = new Set<PipelinePhase>(["creating-asset", "uploading", "starting-pipeline"]);

export function UploadCard({ onStartUpload, phase, disabled }: UploadCardProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [isDragging, setIsDragging] = useState(false);
  const busy = BUSY_PHASES.has(phase) || disabled;

  const handleFiles = useCallback(
    (files: FileList | null) => {
      const file = files?.[0];
      if (file) onStartUpload(file);
    },
    [onStartUpload],
  );

  const handleDrop = useCallback(
    (event: DragEvent<HTMLLabelElement>) => {
      event.preventDefault();
      setIsDragging(false);
      if (busy) return;
      handleFiles(event.dataTransfer.files);
    },
    [busy, handleFiles],
  );

  return (
    <label
      onDragOver={(event) => {
        event.preventDefault();
        if (!busy) setIsDragging(true);
      }}
      onDragLeave={() => setIsDragging(false)}
      onDrop={handleDrop}
      className={`flex h-60 cursor-pointer flex-col items-center justify-center gap-2 rounded-2xl border-2 border-dashed bg-info text-info-foreground transition-all ${
        isDragging ? "border-primary bg-primary/10 brightness-100 ring-2 ring-ring" : ""
      } ${busy ? "pointer-events-none opacity-70" : "hover:brightness-[0.98]"}`}
    >
      {busy ? (
        <Loader2 className="size-6 animate-spin" />
      ) : (
        <UploadCloud className="size-6" />
      )}
      <span className="text-sm font-semibold">
        {busy ? "Working…" : "Upload a video to transcode"}
      </span>
      <span className="text-xs opacity-80">
        MP4 / MOV / WebM — your video is uploaded, transcoded to HLS, and previewed here
      </span>
      <input
        ref={inputRef}
        type="file"
        accept="video/*"
        className="sr-only"
        disabled={busy}
        onChange={(event) => {
          handleFiles(event.target.files);
          event.target.value = "";
        }}
      />
    </label>
  );
}
