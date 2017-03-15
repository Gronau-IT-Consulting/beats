package kinesis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		Name  string
		Input config
		Valid bool
	}{
		{"Valid", config{PartitionKey: "partition", StreamName: "stream", Region: "eu-central-1"}, true},
		{"No partition key", config{PartitionKey: "", StreamName: "stream", Region: "eu-central-1"}, false},
		{"No partition key", config{StreamName: "stream", Region: "eu-central-1"}, false},
		{"No stream name", config{PartitionKey: "partition", StreamName: "", Region: "eu-central-1"}, false},
		{"No stream name", config{PartitionKey: "partition", Region: "eu-central-1"}, false},
		{"No region", config{PartitionKey: "partition", StreamName: "", Region:""}, false},
		{"No region", config{PartitionKey: "partition"}, false},


	}

	for _, test := range tests {
		assert.Equal(t, test.Input.Validate() == nil, test.Valid, test.Name)
	}
}
