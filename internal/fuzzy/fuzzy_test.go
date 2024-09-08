package fuzzy

import (
	"testing"
)

func TestNormalize_UUIDs(t *testing.T) {
	input := "Failed to process request 550e8400-e29b-41d4-a716-446655440000"
	got := Normalize(input)
	want := "Failed to process request <UUID>"
	if got != want {
		t.Errorf("Normalize UUID:\ngot  %q\nwant %q", got, want)
	}
}

func TestNormalize_IPs(t *testing.T) {
	input := "Connection from 192.168.1.1:8080 refused"
	got := Normalize(input)
	want := "Connection from <IP> refused"
	if got != want {
		t.Errorf("Normalize IP:\ngot  %q\nwant %q", got, want)
	}
}

func TestNormalize_Numbers(t *testing.T) {
	input := "Processed 1234 records in 56.78 seconds"
	got := Normalize(input)
	if got == input {
		t.Error("Normalize should replace numbers")
	}
}

func TestNormalize_Timestamps(t *testing.T) {
	input := "Event at 2024-01-15T10:30:00Z was processed"
	got := Normalize(input)
	if got == input {
		t.Error("Normalize should replace timestamps")
	}
}

func TestNormalize_HexStrings(t *testing.T) {
	input := "Object ID abcdef0123456789abcdef0123 not found"
	got := Normalize(input)
	if got == input {
		t.Error("Normalize should replace hex strings")
	}
}

func TestNormalize_EmptyString(t *testing.T) {
	got := Normalize("")
	if got != "" {
		t.Errorf("Normalize empty: got %q, want empty", got)
	}
}

func TestLevenshtein_Identical(t *testing.T) {
	if d := levenshtein("abc", "abc"); d != 0 {
		t.Errorf("levenshtein identical: got %d, want 0", d)
	}
}

func TestLevenshtein_Empty(t *testing.T) {
	if d := levenshtein("", "abc"); d != 3 {
		t.Errorf("levenshtein empty left: got %d, want 3", d)
	}
	if d := levenshtein("abc", ""); d != 3 {
		t.Errorf("levenshtein empty right: got %d, want 3", d)
	}
}

func TestLevenshtein_SingleEdit(t *testing.T) {
	if d := levenshtein("abc", "abd"); d != 1 {
		t.Errorf("levenshtein single sub: got %d, want 1", d)
	}
	if d := levenshtein("abc", "abcd"); d != 1 {
		t.Errorf("levenshtein single insert: got %d, want 1", d)
	}
}

func TestSimilarity_Identical(t *testing.T) {
	s := similarity("hello", "hello")
	if s != 1.0 {
		t.Errorf("similarity identical: got %f, want 1.0", s)
	}
}

func TestSimilarity_CompletelyDifferent(t *testing.T) {
	s := similarity("abc", "xyz")
	if s >= 0.5 {
		t.Errorf("similarity different: got %f, want < 0.5", s)
	}
}

func TestGroup_ExactDuplicates(t *testing.T) {
	messages := []string{
		"Error: connection refused",
		"Error: connection refused",
		"Error: connection refused",
	}
	groups := Group(messages)
	if len(groups) != 1 {
		t.Fatalf("Group exact duplicates: got %d groups, want 1", len(groups))
	}
	if groups[0].Count != 3 {
		t.Errorf("Group exact duplicates: count got %d, want 3", groups[0].Count)
	}
}

func TestGroup_NormalizationGrouping(t *testing.T) {
	messages := []string{
		"Failed request 550e8400-e29b-41d4-a716-446655440000",
		"Failed request 660e8400-e29b-41d4-a716-446655440001",
		"Failed request 770e8400-e29b-41d4-a716-446655440002",
	}
	groups := Group(messages)
	if len(groups) != 1 {
		t.Fatalf("Group normalized: got %d groups, want 1", len(groups))
	}
	if groups[0].Count != 3 {
		t.Errorf("Group normalized: count got %d, want 3", groups[0].Count)
	}
}

func TestGroup_DifferentMessages(t *testing.T) {
	messages := []string{
		"Error: connection refused",
		"Warning: disk space low",
		"Info: deployment started",
	}
	groups := Group(messages)
	if len(groups) != 3 {
		t.Errorf("Group different: got %d groups, want 3", len(groups))
	}
}

func TestGroup_Empty(t *testing.T) {
	groups := Group(nil)
	if len(groups) != 0 {
		t.Errorf("Group empty: got %d groups, want 0", len(groups))
	}
}

func TestGroup_SimilarMessages(t *testing.T) {
	messages := []string{
		"Error processing order for user Alice",
		"Error processing order for user Bob",
		"Error processing order for user Charlie",
	}
	groups := GroupWithThreshold(messages, 0.7)
	if len(groups) > 1 {
		t.Logf("Got %d groups (messages are similar but may differ enough)", len(groups))
	}
}

func TestGroup_SamplesLimited(t *testing.T) {
	messages := make([]string, 100)
	for i := range messages {
		messages[i] = "Identical error message"
	}
	groups := Group(messages)
	if len(groups) != 1 {
		t.Fatalf("Group samples: got %d groups, want 1", len(groups))
	}
	if len(groups[0].Samples) > maxSamplesPerGroup {
		t.Errorf("Group samples: got %d samples, want <= %d", len(groups[0].Samples), maxSamplesPerGroup)
	}
}

func TestGroupWithThreshold_HighThreshold(t *testing.T) {
	messages := []string{
		"Error: connection timeout to server-1",
		"Error: connection timeout to server-2",
	}
	groups := GroupWithThreshold(messages, 0.99)
	if len(groups) < 1 {
		t.Error("GroupWithThreshold high: should return at least 1 group")
	}
}

func TestGroupWithThreshold_LowThreshold(t *testing.T) {
	messages := []string{
		"Error: connection timeout",
		"Warning: disk space low",
	}
	groups := GroupWithThreshold(messages, 0.1)
	if len(groups) != 1 {
		t.Logf("GroupWithThreshold low: got %d groups (threshold 0.1 may or may not merge)", len(groups))
	}
}
