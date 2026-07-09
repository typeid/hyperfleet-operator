package bucket

import "hash/fnv"

// Assigner returns a pgruntime BucketAssigner that hashes the namespace to
// determine the bucket. Resources sharing a cluster land in the same bucket.
func Assigner(bucketCount int) func(ns, name string) int {
	return func(ns, _ string) int {
		h := fnv.New32a()
		h.Write([]byte(ns))
		return int(h.Sum32() % uint32(bucketCount))
	}
}

// All returns all bucket IDs [0, bucketCount).
func All(bucketCount int) []int {
	ids := make([]int, bucketCount)
	for i := range ids {
		ids[i] = i
	}
	return ids
}

// Slice returns the bucket IDs owned by a specific replica.
// bucketCount must be divisible by replicaCount.
func Slice(bucketCount, replicaCount, ordinal int) []int {
	perReplica := bucketCount / replicaCount
	start := ordinal * perReplica
	ids := make([]int, perReplica)
	for i := range ids {
		ids[i] = start + i
	}
	return ids
}
