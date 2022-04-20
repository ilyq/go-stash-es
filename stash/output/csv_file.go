package output

import (
	"encoding/csv"
	"os"

	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
)

type MessageCSVHandler struct {
	f    *os.File
	keys []string
}

func NewCSVHandler(filename string, header []string) (*MessageCSVHandler, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &MessageCSVHandler{
		f: f,
	}, nil
}

func (m *MessageCSVHandler) MappingKeys(keys []string) error {
	m.keys = keys

	w := csv.NewWriter(m.f)
	w.Write(m.keys)
	w.Flush()
	return nil
}

func (m *MessageCSVHandler) Consume(val string) error {
	var data []map[string]interface{}
	if err := jsoniter.Unmarshal([]byte(val), &data); err != nil {
		return err
	}

	logx.Infof("insert %d rows", len(data))

	w := csv.NewWriter(m.f)
	defer w.Flush()

	for _, sources := range data {
		source := sources["_source"].(map[string]interface{})
		res := []string{}

		for _, key := range m.keys {
			c, err := cast.ToStringE(source[key])
			if err != nil {
				c, err := jsoniter.MarshalToString(source[key])
				if err != nil {
					return err
				}
				res = append(res, string(c))
			} else {
				res = append(res, string(c))
			}
		}

		err := w.Write(res)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MessageCSVHandler) Close() error {
	return m.f.Close()
}
