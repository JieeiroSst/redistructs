package redistructs

import "fmt"

var (
	ErrNotFoundCursor     = fmt.Errorf("redisturcts: cannot found the cursor with given id")
	ErrInvalidMetaKeyType = fmt.Errorf("redisturcts: invalid type of meta key")
)
