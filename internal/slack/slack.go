package slack

import (
	"fmt"
	"strings"
	"time"

	"github.com/ricardonunez-io/lumberjack/internal/analyzer"
	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"
)

type Config struct {
	BotToken  string
	ChannelID string
}

func SendMessage(result analyzer.AnalysisResult, config Config) error {
	api := slack.New(config.BotToken)

	severityEmoji := severityToEmoji(result.Severity)

	blocks := []slack.Block{
		slack.NewHeaderBlock(slack.NewTextBlockObject(
			"plain_text",
			fmt.Sprintf("%s Log Analysis — %s", severityEmoji, strings.ToUpper(result.Severity)),
			false, false,
		)),
		slack.NewDividerBlock(),
		slack.NewSectionBlock(
			slack.NewTextBlockObject("mrkdwn",
				fmt.Sprintf("*Signal Strength:* %d/10\n*Severity:* %s",
					result.SignalStrength, result.Severity),
				false, false),
			nil, nil,
		),
		slack.NewSectionBlock(
			slack.NewTextBlockObject("mrkdwn",
				fmt.Sprintf("*Reasoning:*\n%s", result.Reasoning),
				false, false),
			nil, nil,
		),
	}

	if len(result.KeyPoints) > 0 {
		points := make([]string, len(result.KeyPoints))
		for i, p := range result.KeyPoints {
			points[i] = fmt.Sprintf("• %s", p)
		}
		blocks = append(blocks, slack.NewSectionBlock(
			slack.NewTextBlockObject("mrkdwn",
				fmt.Sprintf("*Key Points:*\n%s", strings.Join(points, "\n")),
				false, false),
			nil, nil,
		))
	}

	ts, err := time.Parse(time.RFC3339, result.Timestamp)
	if err == nil {
		blocks = append(blocks, slack.NewContextBlock("",
			slack.NewTextBlockObject("mrkdwn",
				fmt.Sprintf("Analyzed at: %s", ts.Format(time.RFC1123)),
				false, false),
		))
	}

	_, msgTimestamp, err := api.PostMessage(
		config.ChannelID,
		slack.MsgOptionBlocks(blocks...),
	)
	if err != nil {
		log.Err(err).Str("channel", config.ChannelID).Msg("Failed to post Slack message")
		return err
	}

	log.Info().
		Str("channel", config.ChannelID).
		Str("timestamp", msgTimestamp).
		Msg("Summary posted to Slack")
	return nil
}

func severityToEmoji(severity string) string {
	switch strings.ToLower(severity) {
	case "critical":
		return "🔴"
	case "high":
		return "🟠"
	case "medium":
		return "🟡"
	default:
		return "🟢"
	}
}
