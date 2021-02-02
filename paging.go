package redistructs

import (
	"context"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/octo-5/redistructs/util"
)

type Paging struct {
	redis             redis.Cmdable
	metaKey           string
	dataPrefix        string
	dataExpiry        time.Duration
	needRetention     bool
	maxRetentionEntry int64
	retentionAfter    time.Duration
	logger            Logger
	reversed          bool
}

func NewPaging(redis redis.Cmdable, metaKey ...string) *Paging {
	paging := &Paging{
		redis:             redis,
		dataExpiry:        5 * time.Minute,
		maxRetentionEntry: 10000,
		retentionAfter:    15 * time.Minute,
		logger:            DefaultLogger{},
	}
	if len(metaKey) > 0 {
		paging.metaKey = metaKey[0]
		paging.dataPrefix = metaKey[0]
	}
	return paging
}

func (p *Paging) Redis(cmdable redis.Cmdable) *Paging {
	p.redis = cmdable
	return p
}

func (p *Paging) MetaKey(key string) *Paging {
	p.metaKey = key
	p.dataPrefix = key
	return p
}

func (p *Paging) Desc() *Paging {
	p.reversed = true
	return p
}

func (p *Paging) Retention(maxEntry int64, after time.Duration) *Paging {
	if after > 0 {
		p.needRetention = true
	}
	p.maxRetentionEntry = maxEntry
	p.retentionAfter = after
	return p
}

func (p *Paging) DataKeyPrefix(prefix string) *Paging {
	p.dataPrefix = prefix
	return p
}

func (p *Paging) DataExpiry(expiry time.Duration) *Paging {
	p.dataExpiry = expiry
	return p
}

func (p *Paging) Logger(logger Logger) *Paging {
	p.logger = logger
	return p
}

func (p *Paging) Clone() *Paging {
	return &Paging{
		metaKey:           p.metaKey,
		maxRetentionEntry: p.maxRetentionEntry,
		retentionAfter:    p.retentionAfter,
		redis:             p.redis,
		dataPrefix:        p.dataPrefix,
		dataExpiry:        p.dataExpiry,
		logger:            p.logger,
	}
}

func (p *Paging) Delete(ids ...string) *PagingDeleter {
	deleter := &PagingDeleter{
		ctx:    context.Background(),
		paging: p,
		ids:    ids,
	}
	return deleter
}

func (p *Paging) Read() *PagingReader {
	reader := &PagingReader{
		ctx:    context.Background(),
		paging: p,
	}
	return reader
}

func (p *Paging) Write(values interface{}) *PagingWriter {
	writer := &PagingWriter{
		ctx:    context.Background(),
		paging: p,
	}

	if values == nil {
		return writer
	}

	t := util.IndirectValue(values)

	switch k := t.Type().Kind(); k {
	case reflect.Array, reflect.Slice:
		writer.datas = make([]*PagingData, t.Len())
		for i := 0; i < t.Len(); i++ {
			data, err := newPagingData(t.Index(i).Interface(), p.dataPrefix, p.dataExpiry)
			if err != nil {
				panic(err)
			}
			writer.datas[i] = data
		}
	default:
		data, err := newPagingData(values, p.dataPrefix, p.dataExpiry)
		if err != nil {
			panic(err)
		}
		writer.datas = append(writer.datas, data)
	}

	return writer
}
