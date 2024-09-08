package analyzer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/invopop/jsonschema"
)

type AnalysisResult struct {
	SignalStrength int      `json:"signalStrength" jsonschema:"description=Anomaly signal strength from 1-10"`
	SendSummary    bool     `json:"sendSummary" jsonschema:"description=Whether to send an alert to Slack"`
	Severity       string   `json:"severity" jsonschema:"description=One of: low medium high critical"`
	Reasoning      string   `json:"reasoning" jsonschema:"description=Concise explanation of the analysis"`
	KeyPoints      []string `json:"keyPoints" jsonschema:"description=List of key observations"`
	Timestamp      string   `json:"timestamp" jsonschema:"description=ISO 8601 timestamp of the analysis"`
}

func Analyze(ctx context.Context, inputData string, cfg Config) (*AnalysisResult, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("ANTHROPIC_API_KEY is not set")
	}

	client := anthropic.NewClient(
		option.WithAPIKey(cfg.APIKey),
	)

	outputSchema := generateSchema(&AnalysisResult{})

	message, err := client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.Model(cfg.Model),
		MaxTokens: 2048,
		System: []anthropic.TextBlockParam{
			{Text: systemPrompt},
		},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(inputData)),
		},
		OutputConfig: anthropic.OutputConfigParam{
			Format: anthropic.JSONOutputFormatParam{
				Schema: outputSchema,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("anthropic API error: %w", err)
	}

	if len(message.Content) == 0 {
		return nil, fmt.Errorf("empty response from anthropic")
	}

	var responseText string
	for _, block := range message.Content {
		if block.Type == "text" {
			responseText = block.Text
			break
		}
	}

	if responseText == "" {
		return nil, fmt.Errorf("no text content in anthropic response")
	}

	var result AnalysisResult
	if err := json.Unmarshal([]byte(responseText), &result); err != nil {
		return nil, fmt.Errorf("failed to parse structured output: %w", err)
	}

	if result.Timestamp == "" {
		result.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	return &result, nil
}

func generateSchema(v any) map[string]any {
	r := jsonschema.Reflector{
		AllowAdditionalProperties: false,
		DoNotReference:            true,
	}
	s := r.Reflect(v)
	b, _ := json.Marshal(s)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}
