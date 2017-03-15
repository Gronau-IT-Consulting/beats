package kinesis

import (
	"github.com/elastic/beats/libbeat/outputs"
	"errors"
)

type config struct {
	Codec         outputs.CodecConfig `config:"codec"`
	PartitionKey  string `config:"partition_key" validate:"required"`
	StreamName    string `config:"stream_name" validate:"required"`
	Region        string `config:"region" validate:"required"`
}

var (
	defaultConfig = config{
		PartitionKey: "",
		StreamName: "",
		Region: "",
	}
)

func (c *config) Validate() error {
	if len(c.PartitionKey) == 0 {
		return errors.New("no partition key configured")
	}

	if len(c.StreamName) == 0 {
		return errors.New("no stream name configured")
	}

	if len(c.Region) == 0 {
		return errors.New("no region configured")
	}

	return nil
}
