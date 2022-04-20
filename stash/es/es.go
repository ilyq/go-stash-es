package es

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/ilyq69/go-stash-es/stash/config"
	"github.com/tidwall/gjson"
	"github.com/zeromicro/go-zero/core/logx"

	es7 "github.com/elastic/go-elasticsearch/v7"
)

type (
	ConsumeHandler interface {
		MappingKeys(keys []string) error
		Consume(value string) error
		Close() error
	}

	Elasticsearch struct {
		client7 es7.Client
		handler ConsumeHandler

		index          string
		body           string
		size           int
		scrollDuration time.Duration
		Source         []string
	}

	OrderedMap struct {
		Order []string
		Map   map[string]string
	}
)

func (om *OrderedMap) UnmarshalJSON(b []byte) error {
	// https://stackoverflow.com/questions/48293036/prevent-alphabetically-ordering-json-at-marshal
	json.Unmarshal(b, &om.Map)

	index := make(map[string]int)
	for key := range om.Map {
		om.Order = append(om.Order, key)
		esc, _ := json.Marshal(key) //Escape the key
		index[key] = bytes.Index(b, esc)
	}

	sort.Slice(om.Order, func(i, j int) bool { return index[om.Order[i]] < index[om.Order[j]] })
	return nil
}

func NewES(c config.ESConf, handler ConsumeHandler) (*Elasticsearch, error) {
	cfg := es7.Config{
		Addresses:              c.Addr,
		Username:               c.Username,
		Password:               c.Password,
		CloudID:                c.CloudID,
		APIKey:                 c.APIKey,
		ServiceToken:           c.ServiceToken,
		CertificateFingerprint: c.CertificateFingerprint,
	}
	es7Client, err := es7.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	var source []string
	if len(c.Source) > 0 {
		handler.MappingKeys(c.Source)
		source = c.Source
	} else {
		res, err := es7Client.Indices.GetMapping(
			es7Client.Indices.GetMapping.WithIndex(c.Index),
		)
		if err != nil {
			return nil, err
		}

		body := read(res.Body)
		index := strings.ReplaceAll(c.Index, ".", "\\.")
		data := gjson.Get(body, index).Get("mappings.properties").String()

		var o OrderedMap
		json.Unmarshal([]byte(data), &o)

		handler.MappingKeys(o.Order)
		source = c.Source
	}

	return &Elasticsearch{
		client7: *es7Client,
		handler: handler,

		index:          c.Index,
		body:           c.Body,
		size:           c.Size,
		scrollDuration: time.Duration(c.ScrollDuration) * time.Second,
		Source:         source,
	}, err
}

func read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}

func (es *Elasticsearch) Start() error {
	res, err := es.client7.Search(
		es.client7.Search.WithIndex(es.index),
		es.client7.Search.WithBody(bytes.NewReader([]byte(es.body))),
		es.client7.Search.WithSize(es.size),
		es.client7.Search.WithScroll(es.scrollDuration),
		es.client7.Search.WithSourceIncludes(es.Source...),
	)
	if err != nil {
		return err
	}

	if res.IsError() {
		return errors.New("Error response:" + res.String())
	}

	data := read(res.Body)
	res.Body.Close()

	hits := gjson.Get(data, "hits.hits")
	if len(hits.Array()) < 1 {
		logx.Info("Finished")
		return nil
	}

	es.handler.Consume(hits.String())

	for {

		res, err := es.client7.Scroll(es.client7.Scroll.WithScrollID(gjson.Get(data, "_scroll_id").String()), es.client7.Scroll.WithScroll(es.scrollDuration))
		if err != nil {
			return err
		}

		if res.IsError() {
			return errors.New("Error response:" + res.String())
		}

		data = read(res.Body)
		res.Body.Close()

		hits := gjson.Get(data, "hits.hits")
		if len(hits.Array()) < 1 {
			logx.Info("Finished")
			break
		} else {
			es.handler.Consume(hits.String())
		}
	}

	return nil
}

func (es *Elasticsearch) Close() error {
	return es.handler.Close()
}
