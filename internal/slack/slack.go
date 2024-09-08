package slack

import (
	"fmt"
	"strings"
	"time"

	"github.com/ricardonunez-io/lumberjack/internal/laminar"
	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"
)

type Config struct {
	BotToken  string
	ChannelID string
}

func SendMessage(response laminar.LaminarResponse, config Config) error {
	api := slack.New(config.BotToken)

	blocks := []slack.Block{
		slack.NewHeaderBlock(slack.NewTextBlockObject("plain_text", fmt.Sprintf("Log Analysis Summary - %s", response.Severity), false, false)),
		slack.NewDividerBlock(),
		slack.NewSectionBlock(
			slack.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Signal Strength:* %d/10", response.SignalStrength), false, false),
			nil,
			nil,
		),
		slack.NewSectionBlock(
			slack.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Reasoning:* %s", response.Reasoning), false, false),
			nil,
			nil,
		),
	}

	if len(response.KeyPoints) > 0 {
		keyPointsText := "*Key Points:*\n" + strings.Join(addBulletPoints(response.KeyPoints), "\n")
		blocks = append(blocks, slack.NewSectionBlock(
			slack.NewTextBlockObject("mrkdwn", keyPointsText, false, false),
			nil,
			nil,
		))
	}

	timestamp, err := time.Parse(time.RFC3339, response.Timestamp)
	if err == nil {
		blocks = append(blocks, slack.NewContextBlock(
			"",
			slack.NewTextBlockObject("mrkdwn", fmt.Sprintf("Analyzed at: %s", timestamp.Format(time.RFC1123)), false, false),
		))
	}

	_, msgTimestamp, err := api.PostMessage(
		config.ChannelID,
		slack.MsgOptionBlocks(blocks...),
	)

	if err != nil {
		log.Err(err).Msg(fmt.Sprintf("Error posting message to channel %v", config.ChannelID))
		return err
	}

	log.Info().Msg(fmt.Sprintf("Message successfully sent to channel %s at %s", config.ChannelID, msgTimestamp))
	return nil
}

func addBulletPoints(points []string) []string {
	for i, point := range points {
		points[i] = "• " + point
	}
	return points
}
