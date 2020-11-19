package sidb

import "math"

type Comparator func(a, b []byte) int

func BytesComparator(a, b []byte) int {
	lenA, lenB := len(a), len(b)
	n := int(math.Min(float64(lenA), float64(lenB)))
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	if lenA > lenB {
		return 1
	} else if lenA < lenB {
		return -1
	}
	return 0
}
