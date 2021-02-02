package util

import (
	"strings"
)

const keySeperator = ":"

func GenerateKey(tokens ...string) string {
	return strings.Join(tokens, keySeperator)
}

func GetIDFromKey(key string) string {
	tokens := strings.Split(key, keySeperator)
	return tokens[len(tokens)-1]
}
