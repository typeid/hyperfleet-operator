package bucket

import (
	"fmt"
	"testing"
)

func TestAssigner_Deterministic(t *testing.T) {
	assign := Assigner(32)

	for _, ns := range []string{"cluster-abc", "cluster-xyz", "_", ""} {
		first := assign(ns, "resource-1")
		for i := 0; i < 100; i++ {
			if got := assign(ns, fmt.Sprintf("resource-%d", i)); got != first {
				t.Errorf("assign(%q, resource-%d) = %d, want %d (same namespace must give same bucket)", ns, i, got, first)
			}
		}
	}
}

func TestAssigner_Range(t *testing.T) {
	for _, bucketCount := range []int{1, 4, 32, 128} {
		assign := Assigner(bucketCount)
		for i := 0; i < 1000; i++ {
			ns := fmt.Sprintf("cluster-%d", i)
			b := assign(ns, "x")
			if b < 0 || b >= bucketCount {
				t.Errorf("assign(%q) = %d, out of range [0, %d)", ns, b, bucketCount)
			}
		}
	}
}

func TestAssigner_Distribution(t *testing.T) {
	bucketCount := 32
	assign := Assigner(bucketCount)
	counts := make([]int, bucketCount)

	n := 10000
	for i := 0; i < n; i++ {
		counts[assign(fmt.Sprintf("cluster-%d", i), "x")]++
	}

	expected := n / bucketCount
	for b, c := range counts {
		if c < expected/3 || c > expected*3 {
			t.Errorf("bucket %d has %d items (expected ~%d), distribution is poor", b, c, expected)
		}
	}
}

func TestSlice(t *testing.T) {
	tests := []struct {
		bucketCount  int
		replicaCount int
		ordinal      int
		want         []int
	}{
		{32, 2, 0, seq(0, 16)},
		{32, 2, 1, seq(16, 16)},
		{32, 4, 0, seq(0, 8)},
		{32, 4, 3, seq(24, 8)},
		{32, 1, 0, seq(0, 32)},
		{1, 1, 0, []int{0}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d/%d/ord%d", tt.bucketCount, tt.replicaCount, tt.ordinal), func(t *testing.T) {
			got := Slice(tt.bucketCount, tt.replicaCount, tt.ordinal)
			if len(got) != len(tt.want) {
				t.Fatalf("Slice(%d, %d, %d) = %v (len %d), want len %d", tt.bucketCount, tt.replicaCount, tt.ordinal, got, len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("Slice[%d] = %d, want %d", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func seq(start, count int) []int {
	s := make([]int, count)
	for i := range s {
		s[i] = start + i
	}
	return s
}
