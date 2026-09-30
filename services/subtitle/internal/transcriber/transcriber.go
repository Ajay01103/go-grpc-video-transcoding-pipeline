package transcriber

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type TranscriberClient struct {
	serverURL string
	client    *http.Client
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("transcribe failed: status=%d body=%s", e.StatusCode, e.Body)
}

type TranscribeRequest struct {
	AudioFile string `json:"audio_file"`
	Language  string `json:"language"`
}

type TranscribeResponse struct {
	Language string     `json:"language"`
	Segments []Segment  `json:"segments"`
}

type Segment struct {
	ID    int    `json:"id"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
	Text  string `json:"text"`
}

func NewTranscriberClient(serverURL string) *TranscriberClient {
	return &TranscriberClient{
		serverURL: serverURL,
		client: &http.Client{
			Timeout: 30 * time.Minute,
		},
	}
}

func (tc *TranscriberClient) Transcribe(ctx context.Context, audioFile, language string) (*TranscribeResponse, error) {
	reqBody := TranscribeRequest{
		AudioFile: audioFile,
		Language:  language,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/transcribe", tc.serverURL),
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := tc.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("transcribe request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, &HTTPError{StatusCode: resp.StatusCode, Body: string(body)}
	}

	var result TranscribeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return &result, nil
}

func (tc *TranscriberClient) SegmentsToVTT(segments []Segment) string {
	vtt := "WEBVTT\n\n"
	for _, seg := range segments {
		startTime := formatTime(seg.Start)
		endTime := formatTime(seg.End)
		vtt += fmt.Sprintf("%s --> %s\n%s\n\n", startTime, endTime, seg.Text)
	}
	return vtt
}

func formatTime(seconds float64) string {
	h := int(seconds) / 3600
	m := (int(seconds) % 3600) / 60
	s := int(seconds) % 60
	ms := int((seconds - float64(int(seconds))) * 1000)
	return fmt.Sprintf("%02d:%02d:%02d.%03d", h, m, s, ms)
}
