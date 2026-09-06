package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type ProbeResult struct {
	Duration   float64 `json:"duration"`
	Width      int     `json:"width"`
	Height     int     `json:"height"`
	VideoCodec string  `json:"video_codec"`
	AudioCodec string  `json:"audio_codec"`
	BitRate    int64   `json:"bit_rate"`
}

type TranscodeJob struct {
	AssetID    string
	SourceFile string
	OutputDir  string
	Renditions []Rendition
}

type Rendition struct {
	Name      string
	Width     int
	Height    int
	BitRate   string
	FrameRate string
}

type MediaWorker struct {
	ffprobePath string
	ffmpegPath  string
}

func NewMediaWorker() *MediaWorker {
	return &MediaWorker{
		ffprobePath: "ffprobe",
		ffmpegPath:  "ffmpeg",
	}
}

func (w *MediaWorker) ProbeFile(ctx context.Context, filePath string) (*ProbeResult, error) {
	cmd := exec.CommandContext(ctx, w.ffprobePath,
		"-v", "error",
		"-show_entries", "stream=duration,width,height,codec_name,codec_type,bit_rate",
		"-of", "json",
		filePath,
	)

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	var probeData struct {
		Streams []map[string]interface{} `json:"streams"`
	}
	if err := json.Unmarshal(output, &probeData); err != nil {
		return nil, fmt.Errorf("parse ffprobe output: %w", err)
	}

	result := &ProbeResult{}
	for _, stream := range probeData.Streams {
		if codecType, ok := stream["codec_type"].(string); ok {
			switch codecType {
			case "video":
				if w, ok := stream["width"].(float64); ok {
					result.Width = int(w)
				}
				if h, ok := stream["height"].(float64); ok {
					result.Height = int(h)
				}
				if codec, ok := stream["codec_name"].(string); ok {
					result.VideoCodec = codec
				}
				if d, ok := stream["duration"].(string); ok {
					fmt.Sscanf(d, "%f", &result.Duration)
				}
				if br, ok := stream["bit_rate"].(string); ok {
					fmt.Sscanf(br, "%d", &result.BitRate)
				}
			case "audio":
				if codec, ok := stream["codec_name"].(string); ok {
					result.AudioCodec = codec
				}
			}
		}
	}
	return result, nil
}

func (w *MediaWorker) TranscodeLadder(ctx context.Context, job TranscodeJob) error {
	if len(job.Renditions) == 0 {
		return fmt.Errorf("no renditions specified")
	}

	if err := os.MkdirAll(job.OutputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	args := []string{
		"-i", job.SourceFile,
		"-preset", "medium",
	}

	filterParts := make([]string, 0, len(job.Renditions)+1)
	filterParts = append(filterParts, fmt.Sprintf("[0:v]split=%d", len(job.Renditions)))
	for i, r := range job.Renditions {
		filterParts[0] += fmt.Sprintf("[vsrc%d]", i)
		scale := fmt.Sprintf("[vsrc%d]scale=w=%d:h=%d[v%d]", i, r.Width, r.Height, i)
		filterParts = append(filterParts, scale)
	}
	args = append(args, "-filter_complex", strings.Join(filterParts, ";"))
	for i := range job.Renditions {
		args = append(args, "-map", fmt.Sprintf("[v%d]", i), "-map", "0:a:0")
	}

	varStreamMap := []string{}
	for i, r := range job.Renditions {
		varStreamMap = append(varStreamMap, fmt.Sprintf("v:%d,a:%d", i, i))
		args = append(args,
			"-c:v:"+fmt.Sprint(i), "libx264",
			"-c:a:"+fmt.Sprint(i), "aac",
			"-b:v:"+fmt.Sprint(i), r.BitRate,
			"-maxrate:v:"+fmt.Sprint(i), r.BitRate,
			"-bufsize:v:"+fmt.Sprint(i), r.BitRate,
		)
	}

	args = append(args,
		"-var_stream_map", strings.Join(varStreamMap, " "),
		"-f", "hls",
		"-hls_time", "6",
		"-hls_playlist_type", "vod",
		"-hls_segment_type", "fmp4",
		"-master_pl_name", "master.m3u8",
		filepath.Join(job.OutputDir, "stream_%v/segment_%05d.m4s"),
	)

	cmd := exec.CommandContext(ctx, w.ffmpegPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("transcode failed: %w", err)
	}
	return nil
}

func (w *MediaWorker) GenerateThumbnail(ctx context.Context, sourceFile, outputFile string, frameTime string) error {
	cmd := exec.CommandContext(ctx, w.ffmpegPath,
		"-i", sourceFile,
		"-ss", frameTime,
		"-vframes", "1",
		"-vf", "scale=1280:-1",
		"-f", "image2",
		outputFile,
	)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("generate thumbnail failed: %w", err)
	}
	return nil
}

func (w *MediaWorker) GenerateStoryboard(ctx context.Context, sourceFile, outputDir string, interval, tileWidth, tileHeight int) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	spriteFile := filepath.Join(outputDir, "sprite_%04d.jpg")
	cmd := exec.CommandContext(ctx, w.ffmpegPath,
		"-i", sourceFile,
		"-vf", fmt.Sprintf("fps=1/%d,scale=%d:%d,tile=%dx?", interval, tileWidth, tileHeight, tileWidth/tileHeight),
		spriteFile,
	)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("generate storyboard failed: %w", err)
	}

	// Generate VTT file with sprite references
	vttFile := filepath.Join(outputDir, "storyboard.vtt")
	vttContent := "WEBVTT\n\n"

	prob, err := w.ProbeFile(ctx, sourceFile)
	if err != nil {
		return fmt.Errorf("probe for vtt generation: %w", err)
	}

	spritesPerRow := tileWidth / 120
	spriteIndex := 0
	tileIndex := 0

	for t := 0.0; t < prob.Duration; t += float64(interval) {
		spriteNum := spriteIndex / spritesPerRow
		xPos := (tileIndex % spritesPerRow) * 120
		yPos := (tileIndex / spritesPerRow) * 68

		fromTime := fmt.Sprintf("%02d:%02d:%02d.000", int(t)/3600, (int(t)%3600)/60, int(t)%60)
		toTime := fmt.Sprintf("%02d:%02d:%02d.000", int(t+float64(interval))/3600, (int(t+float64(interval))%3600)/60, int(t+float64(interval))%60)

		vttContent += fmt.Sprintf("%s --> %s\nsprite_%04d.jpg#xywh=%d,%d,120,68\n\n", fromTime, toTime, spriteNum, xPos, yPos)
		tileIndex++
		if tileIndex >= (tileWidth/120)*(tileHeight/68) {
			tileIndex = 0
			spriteIndex++
		}
	}

	return os.WriteFile(vttFile, []byte(vttContent), 0644)
}

func (w *MediaWorker) CopyFileToOutput(ctx context.Context, src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file: %w", err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("copy file: %w", err)
	}
	return nil
}
