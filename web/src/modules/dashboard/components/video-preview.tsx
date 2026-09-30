"use client";

import { useEffect, useState } from "react";

import type {
  MediaPlayer,
  MediaProvider,
  Poster,
  Track,
} from "@vidstack/react";
import type { DefaultVideoLayout } from "@vidstack/react/player/layouts/default";
import { defaultLayoutIcons } from "@vidstack/react/player/layouts/default";
// Tailwind v4's CSS resolver can't follow @vidstack/react's wildcard exports
// map (CSS-syntax @import in globals.css fails), but Turbopack resolves these
// fine from TS.
import "@vidstack/react/player/styles/default/theme.css";
import "@vidstack/react/player/styles/default/layouts/video.css";

import type { PlayerConfigView } from "../api/use-video-pipeline";

interface PlayerBundle {
  MediaPlayer: typeof MediaPlayer;
  MediaProvider: typeof MediaProvider;
  Poster: typeof Poster;
  Track: typeof Track;
  VideoLayout: typeof DefaultVideoLayout;
}

/**
 * Vidstack player with the Default Layout, loaded client-side only (the
 * library needs Web APIs for its HLS provider and custom elements).
 * Src/poster/storyboard/captions come from the playback service's
 * GetPlayerConfig as direct public RustFS URLs (or signed CDN URLs in prod).
 */
export function VideoPreview({ config }: { config: PlayerConfigView }) {
  const [playerBundle, setPlayerBundle] = useState<PlayerBundle | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const [
        { MediaPlayer, MediaProvider, Poster, Track },
        { DefaultVideoLayout },
      ] = await Promise.all([
        import("@vidstack/react"),
        import("@vidstack/react/player/layouts/default"),
      ]);
      if (!cancelled) {
        setPlayerBundle({
          MediaPlayer,
          MediaProvider,
          Poster,
          Track,
          VideoLayout: DefaultVideoLayout,
        });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  if (!playerBundle) {
    return <PlayerSkeleton />;
  }

  const {
    MediaPlayer: PlayerComponent,
    MediaProvider: ProviderComponent,
    Poster: PosterComponent,
    Track: TrackComponent,
    VideoLayout,
  } = playerBundle;

  return (
    <div className="relative aspect-video w-full overflow-hidden rounded-2xl border border-border bg-black shadow-2xl">
      <PlayerComponent
        className="size-full aspect-video"
        src={config.src}
        poster={config.poster || undefined}
        title={config.playbackId}
        playsInline
        crossOrigin
      >
        <ProviderComponent>
          {config.poster && (
            <PosterComponent
              className="vds-poster"
              src={config.poster}
              alt="Video preview poster"
            />
          )}
          {config.subtitles.map((track) => (
            <TrackComponent
              key={track.lang}
              content=""
              kind="subtitles"
              src={track.url}
              lang={track.lang}
              label={track.label}
              default={false}
            />
          ))}
        </ProviderComponent>
        <VideoLayout
          icons={defaultLayoutIcons}
          thumbnails={config.storyboardSrc || undefined}
        />
      </PlayerComponent>
    </div>
  );
}

function PlayerSkeleton() {
  return (
    <div className="flex aspect-video w-full items-center justify-center rounded-2xl border border-border bg-card">
      <span className="text-sm text-muted-foreground">Loading player…</span>
    </div>
  );
}
