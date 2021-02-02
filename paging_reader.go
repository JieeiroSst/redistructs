package redistructs

import (
	"context"

	"github.com/octo-5/redistructs/util"
)

type PagingReader struct {
	ctx      context.Context
	paging   *Paging
	onlyMeta bool
	lastID   string
	count    int64
	keys     []string
}

func (r *PagingReader) With(ctx context.Context) *PagingReader {
	r.ctx = ctx
	return r
}

func (r *PagingReader) OnlyMeta() *PagingReader {
	r.onlyMeta = true
	return r
}

func (r *PagingReader) Page(count int64, lastID ...string) *PagingReader {
	r.count = count
	if len(lastID) > 0 {
		r.lastID = lastID[0]
	}
	return r
}

func (r *PagingReader) IDs(ids ...string) *PagingReader {
	r.keys = make([]string, len(ids))
	for i, id := range ids {
		r.keys[i] = util.GenerateKey(r.paging.dataPrefix, id)
	}
	return r
}

func (r *PagingReader) Exec() ([]CachingResult, error) {
	operator := pangingOperator{
		ctx:     r.ctx,
		metaKey: r.paging.metaKey,
	}

	var res []CachingResult

	if r.count > 0 {
		page, err := operator.readPage(
			r.paging.redis,
			r.paging.dataPrefix,
			r.lastID,
			r.count,
			r.onlyMeta,
			r.paging.reversed,
		)
		if err != nil {
			return nil, err
		}
		res = page
	} else if len(r.keys) > 0 {
		if r.onlyMeta {
			metas, err := operator.readMeta(r.paging.redis, r.keys...)
			if err != nil {
				return nil, err
			}
			res = metas
		} else {
			datas, err := operator.readData(r.paging.redis, r.keys...)
			if err != nil {
				return nil, err
			}
			res = datas
		}
	}

	return res, nil
}
