package output

import (
	"bufio"
	"encoding/json"
	"os"

	jsoniter "github.com/json-iterator/go"
	"github.com/zeromicro/go-zero/core/logx"
)

type MessageJsonFileHandler struct {
	f *os.File
}

func NewJsonFileHandler(filename string) (*MessageJsonFileHandler, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return &MessageJsonFileHandler{
		f: f,
	}, nil
}

func (m *MessageJsonFileHandler) MappingKeys(keys []string) error {
	return nil
}

func (m *MessageJsonFileHandler) Consume(val string) error {
	var data []map[string]interface{}
	if err := jsoniter.Unmarshal([]byte(val), &data); err != nil {
		return err
	}

	logx.Infof("insert %d rows", len(data))

	w := bufio.NewWriter(m.f)

	for _, sources := range data {
		source, err := json.Marshal(sources["_source"])
		if err != nil {
			return err
		}
		_, err = w.Write(source)
		if err != nil {
			return err
		}
		w.Write([]byte("\n"))
	}
	return w.Flush()
}

func (m *MessageJsonFileHandler) Close() error {
	return m.f.Close()
}
