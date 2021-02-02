package redistructs

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
	"github.com/octo-5/redistructs/util"
)

type PagingDeleter struct {
	ctx      context.Context
	paging   *Paging
	ids      []string
	onlyMeta bool
}

func (d *PagingDeleter) With(ctx context.Context) *PagingDeleter {
	d.ctx = ctx
	return d
}

func (d *PagingDeleter) OnlyMeta() *PagingDeleter {
	d.onlyMeta = true
	return d
}

func (d *PagingDeleter) Exec() error {
	if len(d.ids) == 0 {
		return nil
	}

	operator := pangingOperator{
		ctx:     d.ctx,
		metaKey: d.paging.metaKey,
	}

	keys := make([]string, len(d.ids))
	for _, id := range d.ids {
		keys = append(keys, util.GenerateKey(d.paging.dataPrefix, id))
	}

	txPipe := d.paging.redis.TxPipeline()
	defer txPipe.Close()

	if err := operator.deleteMeta(txPipe, keys...); err != nil {
		return err
	}

	if !d.onlyMeta {
		if err := operator.deleteData(txPipe, keys...); err != nil {
			return err
		}
	}
	_, err := txPipe.Exec(d.ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}
