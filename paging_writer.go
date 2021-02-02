package redistructs

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type PagingWriter struct {
	ctx      context.Context
	paging   *Paging
	datas    []*PagingData
	onlyMeta bool
}

func (w *PagingWriter) With(ctx context.Context) *PagingWriter {
	w.ctx = ctx
	return w
}

func (w *PagingWriter) OnlyMeta() *PagingWriter {
	w.onlyMeta = true
	return w
}

func (w *PagingWriter) Exec(overWrite ...bool) error {
	operator := pangingOperator{
		ctx:     w.ctx,
		metaKey: w.paging.metaKey,
	}

	if w.paging.needRetention {
		operator.tryRetention(
			w.paging.redis,
			w.paging.maxRetentionEntry,
			w.paging.dataExpiry,
			w.paging.reversed,
			w.paging.logger,
		)
	}

	if len(w.datas) == 0 {
		return nil
	}

	forceWrite := false
	if len(overWrite) > 0 {
		forceWrite = overWrite[0]
	}

	metas := make([]*redis.Z, len(w.datas))
	for i, v := range w.datas {
		metas[i] = v.meta
	}

	txPipe := w.paging.redis.TxPipeline()
	defer txPipe.Close()

	err := operator.writeMeta(txPipe, metas, forceWrite)
	if err != nil {
		return err
	}

	if !w.onlyMeta {
		err = operator.writeData(txPipe, w.datas, forceWrite, w.paging.dataExpiry)
		if err != nil {
			return err
		}
	}

	_, err = txPipe.Exec(w.ctx)
	return err
}
