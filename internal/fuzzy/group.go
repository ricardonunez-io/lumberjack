package fuzzy

import "sort"

type MessageGroup struct {
	Template string
	Count    int
	Samples  []string
}

const DefaultSimilarityThreshold = 0.85
const maxSamplesPerGroup = 3

func Group(messages []string) []MessageGroup {
	return GroupWithThreshold(messages, DefaultSimilarityThreshold)
}

func GroupWithThreshold(messages []string, threshold float64) []MessageGroup {
	templateGroups := make(map[string]*MessageGroup)
	var ungrouped []string

	for _, msg := range messages {
		norm := Normalize(msg)
		if g, ok := templateGroups[norm]; ok {
			g.Count++
			if len(g.Samples) < maxSamplesPerGroup {
				g.Samples = append(g.Samples, msg)
			}
		} else {
			templateGroups[norm] = &MessageGroup{
				Template: norm,
				Count:    1,
				Samples:  []string{msg},
			}
		}
	}

	var groups []*MessageGroup
	for _, g := range templateGroups {
		groups = append(groups, g)
	}

	merged := mergeByLevenshtein(groups, threshold)

	for _, g := range merged {
		if g.Count == 0 {
			ungrouped = append(ungrouped, g.Samples...)
		}
	}
	_ = ungrouped

	result := make([]MessageGroup, 0, len(merged))
	for _, g := range merged {
		if g.Count > 0 {
			result = append(result, *g)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Count > result[j].Count
	})

	return result
}

func mergeByLevenshtein(groups []*MessageGroup, threshold float64) []*MessageGroup {
	if len(groups) <= 1 {
		return groups
	}

	for i := 0; i < len(groups); i++ {
		if groups[i].Count == 0 {
			continue
		}
		for j := i + 1; j < len(groups); j++ {
			if groups[j].Count == 0 {
				continue
			}
			sim := similarity(groups[i].Template, groups[j].Template)
			if sim >= threshold {
				groups[i].Count += groups[j].Count
				for _, s := range groups[j].Samples {
					if len(groups[i].Samples) < maxSamplesPerGroup {
						groups[i].Samples = append(groups[i].Samples, s)
					}
				}
				groups[j].Count = 0
				groups[j].Samples = nil
			}
		}
	}

	return groups
}

func similarity(a, b string) float64 {
	if a == b {
		return 1.0
	}
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	if maxLen == 0 {
		return 1.0
	}
	dist := levenshtein(a, b)
	return 1.0 - float64(dist)/float64(maxLen)
}

func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	prev := make([]int, lb+1)
	curr := make([]int, lb+1)

	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min(curr[j-1]+1, min(prev[j]+1, prev[j-1]+cost))
		}
		prev, curr = curr, prev
	}

	return prev[lb]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
