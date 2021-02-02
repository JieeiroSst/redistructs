package redistructs

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/octo-5/redistructs/util"
)

var (
	tagName        = "redistructs"
	pagingTagID    = "id"
	pagingTagScore = "score"
	pagingAllTags  = []string{pagingTagID, pagingTagScore}
)

type PagingData struct {
	meta *redis.Z

	id     string
	value  string
	expiry time.Duration
}

func newPagingData(iface interface{}, prefix string, expiry time.Duration) (*PagingData, error) {
	data := &PagingData{
		expiry: expiry,
	}

	name, tags, err := util.ExtractTags(iface, tagName, pagingAllTags...)
	if err != nil {
		return nil, err
	}

	var id string
	var score float64

	for _, v := range tags {
		if util.StringsContains(v.Tags, pagingTagID) {
			id = fmt.Sprintf("%v", v.Val)
		}

		if util.StringsContains(v.Tags, pagingTagScore) {
			score = convertToScore(v.Val)
		}
	}

	if id == "" {
		panic(fmt.Errorf("no paging id tag is defined in '%s' type", name))
	}

	val, err := json.Marshal(iface)
	if err != nil {
		panic(fmt.Errorf("newPagingData: %s: %w", name, err))
	}

	data.meta = &redis.Z{Score: score, Member: util.GenerateKey(prefix, id)}
	data.value = string(val)
	return data, nil
}

func convertToScore(iface interface{}) float64 {
	switch t := iface.(type) {
	case int:
		return float64(t)
	case int8:
		return float64(t)
	case int16:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint:
		return float64(t)
	case uint8:
		return float64(t)
	case uint16:
		return float64(t)
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	case float32:
		return float64(t)
	case float64:
		return float64(t)
	case time.Time:
		return float64(t.Unix())
	default:
		return float64(time.Now().Unix())
	}
}
