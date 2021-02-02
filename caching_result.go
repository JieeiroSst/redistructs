package redistructs

import (
	"encoding/json"
	"fmt"

	"github.com/octo-5/redistructs/util"
)

type CachingResult struct {
	err     error
	key     string
	rawData interface{}
}

func (c *CachingResult) ID() string {
	return util.GetIDFromKey(c.key)
}

func (c *CachingResult) Key() string {
	return c.key
}

func (c *CachingResult) Raw() interface{} {
	return c.rawData
}

func (c *CachingResult) ScanData(v interface{}) error {
	if jsonStr, ok := c.rawData.(string); ok {
		return json.Unmarshal([]byte(jsonStr), v)
	}
	//TODO
	return fmt.Errorf("")
}

func (c *CachingResult) Err() error {
	return c.err
}
