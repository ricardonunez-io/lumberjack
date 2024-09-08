package laminar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Config struct {
	LaminarKey   string
	AnthropicKey string
}

type LaminarRequest struct {
	Pipeline string            `json:"pipeline"`
	Inputs   map[string]string `json:"inputs"`
	Env      map[string]string `json:"env"`
	Metadata map[string]string `json:"metadata"`
	Stream   bool              `json:"stream"`
}

type LaminarResponse struct {
	SignalStrength int      `json:"signalStrength"`
	SendSummary    bool     `json:"sendSummary"`
	Reasoning      string   `json:"reasoning"`
	KeyPoints      []string `json:"keyPoints"`
	Severity       Severity `json:"severity"`
	Timestamp      string   `json:"timestamp"`
}

type Severity int

const (
	Low Severity = iota
	Medium
	High
	Critical
)

func (s Severity) String() string {
	return [...]string{"Low", "Medium", "High", "Critical"}[s]
}

func Run(inputData string, config Config) (*LaminarResponse, error) {
	url := "https://api.lmnr.ai/v1/pipeline/run"

	if config.LaminarKey == "" || config.AnthropicKey == "" {
		return nil, fmt.Errorf("LAMINAR_API_KEY or ANTHROPIC_API_KEY is not set")
	}

	request := LaminarRequest{
		Pipeline: "Lumberjack Analyzer",
		Inputs: map[string]string{
			"input_data": inputData,
		},
		Env: map[string]string{
			"ANTHROPIC_API_KEY": config.AnthropicKey,
		},
		Metadata: map[string]string{},
		Stream:   false,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+config.LaminarKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var laminarRes LaminarResponse
	err = json.Unmarshal(body, &laminarRes)
	if err != nil {
		return nil, fmt.Errorf("received invalid response from Laminar: %v", err.Error())
	}
	return &laminarRes, nil
}
