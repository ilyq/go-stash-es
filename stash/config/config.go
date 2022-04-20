package config

import "github.com/zeromicro/go-zero/core/service"

type (
	ESConf struct {
		service.ServiceConf
		Addr           []string
		Index          string
		Body           string
		Size           int      `json:",default=10000"` // query body size
		ScrollDuration int      `json:",default=60"`
		Source         []string `json:",optional"`

		Username               string `json:",optional"`
		Password               string `json:",optional"`
		CloudID                string `json:",optional"`
		APIKey                 string `json:",optional"`
		ServiceToken           string `json:",optional"`
		CertificateFingerprint string `json:",optional"`
	}

	Output struct {
		Target   string `json:",options=json|csv"`
		Filename string
	}

	Config struct {
		ESConf ESConf
		Output Output
	}
)
