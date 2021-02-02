package util

func StringsContains(arr []string, val string) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}

func StringsIntersects(a, b []string) []string {
	intersects := []string{}
	for _, v := range b {
		if StringsContains(a, v) {
			intersects = append(intersects, v)
		}
	}
	return intersects
}
