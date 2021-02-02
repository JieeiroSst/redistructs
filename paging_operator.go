package redistructs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/octo-5/redistructs/util"
)

const retentionFlag = "redistructs:retention"

type pangingOperator struct {
	metaKey string
	ctx     context.Context
}

func (o *pangingOperator) deleteMeta(cmd redis.Cmdable, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	ifaceKeys := make([]interface{}, len(keys))
	for _, key := range keys {
		ifaceKeys = append(ifaceKeys, key)
	}
	return cmd.ZRem(o.ctx, o.metaKey, ifaceKeys...).Err()
}

func (o *pangingOperator) deleteData(cmd redis.Cmdable, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return cmd.Unlink(o.ctx, keys...).Err()
}

func (o *pangingOperator) readPage(cmd redis.Cmdable, dataKeyPrefix, lastID string, count int64, onlyMeta, reversed bool) ([]CachingResult, error) {
	var start int64 = 0

	if lastID != "" {
		var index *redis.IntCmd

		cursor := util.GenerateKey(dataKeyPrefix, lastID)

		if reversed {
			index = cmd.ZRevRank(o.ctx, o.metaKey, cursor)
		} else {
			index = cmd.ZRank(o.ctx, o.metaKey, cursor)
		}

		if errors.Is(index.Err(), redis.Nil) {
			return nil, fmt.Errorf("%w: %s", ErrNotFoundCursor, cursor)
		}
		start = index.Val() + 1
	}

	var list *redis.StringSliceCmd

	if reversed {
		list = cmd.ZRevRange(o.ctx, o.metaKey, start, start+count-1)
	} else {
		list = cmd.ZRange(o.ctx, o.metaKey, start, start+count-1)
	}

	if err := list.Err(); err != nil {
		return nil, err
	}

	if onlyMeta {
		return o.readMeta(cmd, list.Val()...)
	} else {
		return o.readData(cmd, list.Val()...)
	}
}

func (o *pangingOperator) readMeta(cmd redis.Cmdable, keys ...string) ([]CachingResult, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	pipe := cmd.Pipeline()
	defer pipe.Close()

	pipeResults := make([]*redis.FloatCmd, len(keys))
	cachingResults := make([]CachingResult, len(keys))

	for i, key := range keys {
		pipeResults[i] = pipe.ZScore(o.ctx, o.metaKey, key)
		cachingResults[i] = CachingResult{
			key: key,
		}
	}

	_, err := pipe.Exec(o.ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for i, res := range pipeResults {
		if err := res.Err(); err != nil {
			cachingResults[i].err = err
			continue
		}
		cachingResults[i].rawData = res.Val()
	}
	return cachingResults, nil
}

func (o *pangingOperator) readData(cmd redis.Cmdable, keys ...string) ([]CachingResult, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	pipe := cmd.Pipeline()
	defer pipe.Close()

	pipeResults := make([]*redis.StringCmd, len(keys))
	cachingResults := make([]CachingResult, len(keys))

	for i, key := range keys {
		pipeResults[i] = pipe.Get(o.ctx, key)
		cachingResults[i] = CachingResult{
			key: key,
		}
	}

	_, err := pipe.Exec(o.ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for i, res := range pipeResults {
		if err := res.Err(); err != nil {
			cachingResults[i].err = err
			continue
		}
		cachingResults[i].rawData = res.Val()
	}
	return cachingResults, nil
}

func (o *pangingOperator) writeMeta(cmd redis.Cmdable, metas []*redis.Z, overWrite bool) error {
	if len(metas) == 0 {
		return nil
	}
	var err error
	if overWrite {
		err = cmd.ZAdd(o.ctx, o.metaKey, metas...).Err()
	} else {
		err = cmd.ZAddNX(o.ctx, o.metaKey, metas...).Err()
	}
	return err
}

func (o *pangingOperator) writeData(cmd redis.Cmdable, datas []*PagingData, overWrite bool, expiry time.Duration) error {
	if len(datas) == 0 {
		return nil
	}

	for _, v := range datas {
		dataKey, ok := v.meta.Member.(string)
		if !ok {
			return ErrInvalidMetaKeyType
		}

		var err error
		if overWrite {
			err = cmd.Set(o.ctx, dataKey, v.value, expiry).Err()
		} else {
			err = cmd.SetNX(o.ctx, dataKey, v.value, expiry).Err()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *pangingOperator) existsRentenionFlag(cmd redis.Cmdable) bool {
	retentionKey := util.GenerateKey(retentionFlag, o.metaKey)
	exists := cmd.Exists(o.ctx, retentionKey)
	return exists.Val() != 0
}

func (o *pangingOperator) tryRetention(cmd redis.Cmdable, count int64, pending time.Duration, isReversed bool, logger Logger) {
	if o.existsRentenionFlag(cmd) {
		return
	}

	var err error

	if isReversed {
		err = o.reversedRetention(cmd, count)
	} else {
		err = o.retention(cmd, count)
	}

	if err != nil {
		logger.Errorf("failed to retention on %s: %s", o.metaKey, err)
	}

	key := util.GenerateKey(retentionFlag, o.metaKey)
	if err := cmd.SetNX(o.ctx, key, nil, pending).Err(); err != nil {
		logger.Errorf("failed to set retention flag on %s: %s", o.metaKey, err)
	}
}

func (o *pangingOperator) reversedRetention(cmd redis.Cmdable, count int64) error {
	memberCnt := cmd.ZCard(o.ctx, o.metaKey)
	if err := memberCnt.Err(); err != nil {
		return err
	}

	fmt.Println("memberCnt: ", memberCnt, count)
	if memberCnt.Val() <= count {
		return nil
	}

	txPipe := cmd.TxPipeline()
	defer txPipe.Close()

	if count > 0 {
		txPipe.ZRemRangeByRank(o.ctx, o.metaKey, 0, memberCnt.Val()-count-1)
	} else {
		txPipe.Del(o.ctx, o.metaKey)
	}

	_, err := txPipe.Exec(o.ctx)
	return err
}

func (o *pangingOperator) retention(cmd redis.Cmdable, count int64) error {
	txPipe := cmd.TxPipeline()
	defer txPipe.Close()

	if count > 0 {
		txPipe.ZRemRangeByRank(o.ctx, o.metaKey, count, -1)
	} else {
		txPipe.Del(o.ctx, o.metaKey)
	}

	_, err := txPipe.Exec(o.ctx)
	return err
}
