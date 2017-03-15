package kinesis

import (
	"testing"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/common"
	"github.com/pkg/errors"
)

func TestClose(t *testing.T) {
	out := &output{}
	err := out.Close()
	if err != nil {
		t.Errorf("Not expected error returned: %+v ", err)
	}
}

func TestPublishEventSuccessful(t *testing.T) {
	svc := &kinesisClientMock{}
	itemProcessed := false
	svc.putRecordFunc = func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
		itemProcessed = true
		return &kinesis.PutRecordOutput{}, nil
	}

	out := &output{
		svc: svc,
		config: &config{
			PartitionKey: "partition",
		},
	}
	codec, err := outputs.CreateEncoder(outputs.CodecConfig{})
	if err != nil {
		t.Errorf("Cannot create encoder: %+v", err)
	}
	out.codec = codec

	sig := &signalerMock{}
	sig.failedFunc = func() {
		t.Error("Unexpected failed signal.")
	}
	sig.canceledFunc = func() {
		t.Error("Unexpected canceled signal.")
	}
	sig.completedFunc = func() {}

	out.PublishEvent(sig, outputs.Options{}, getDummyItem())

	if !itemProcessed {
		t.Error("Item not processed")
	}
}

func TestPublishEventFail(t *testing.T) {
	svc := &kinesisClientMock{}
	itemProcessed := false
	svc.putRecordFunc = func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
		return &kinesis.PutRecordOutput{}, errors.New("dummy error")
	}

	out := &output{
		svc: svc,
		config: &config{
			PartitionKey: "partition",
		},
	}
	codec, err := outputs.CreateEncoder(outputs.CodecConfig{})
	if err != nil {
		t.Errorf("Cannot create encoder: %+v", err)
	}
	out.codec = codec

	sig := &signalerMock{}
	sig.failedFunc = func() {
	}
	sig.canceledFunc = func() {
		t.Error("Unexpected canceled signal.")
	}
	sig.completedFunc = func() {
		t.Error("Unexpected completed signal.")
	}

	out.PublishEvent(sig, outputs.Options{}, getDummyItem())

	if itemProcessed {
		t.Error("Item marked as processed, but should not be")
	}

}

func TestBulkPublishEventsExceedsMaxLimitWillSplitData(t *testing.T) {
	testCases := map[string]int{
		"single step": 70,
		"multiple steps completely processed inside loop": 300,
		"multiple steps, some items remaining to be processed after loop": 263,
	}
	for testCase, countItems := range testCases {
		itemsProcessed := 0
		svc := &kinesisClientMock{}
		svc.putRecordsFunc = func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			itemsProcessed += len(input.Records)
			return &kinesis.PutRecordsOutput{}, nil
		}

		out := &output{
			svc: svc,
			config: &config{
				PartitionKey: "partition",
			},
		}
		codec, err := outputs.CreateEncoder(outputs.CodecConfig{})
		if err != nil {
			t.Errorf("%+s: Cannot create encoder: %+v", testCase, err)
		}
		out.codec = codec

		data := []outputs.Data{}
		for i := 0; i < countItems; i++ {
			data = append(data, getDummyItem())
		}
		sig := &signalerMock{}
		sig.failedFunc = func() {
			t.Errorf("%+s: Unexpected failed signal.", testCase)
		}
		sig.canceledFunc = func() {
			t.Errorf("%+s: Unexpected canceled signal.", testCase)
		}
		sig.completedFunc = func() {}

		out.BulkPublish(sig, outputs.Options{}, data)

		if itemsProcessed != countItems {
			t.Errorf("Not processed completely, missing %+v", countItems-itemsProcessed)
		}
	}
}

func TestBulkPublishEventsFail(t *testing.T) {
	testCases := map[string]int{
		"single step": 70,
		"multiple steps completely processed inside loop": 300,
		"multiple steps, some items remaining to be processed after loop": 263,
	}
	for testCase, countItems := range testCases {
		svc := &kinesisClientMock{}
		svc.putRecordsFunc = func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			return &kinesis.PutRecordsOutput{}, errors.New("dummy error")
		}

		out := &output{
			svc: svc,
			config: &config{
				PartitionKey: "partition",
			},
		}
		codec, err := outputs.CreateEncoder(outputs.CodecConfig{})
		if err != nil {
			t.Errorf("%+s: Cannot create encoder: %+v", testCase, err)
		}
		out.codec = codec

		data := []outputs.Data{}
		for i := 0; i < countItems; i++ {
			data = append(data, getDummyItem())
		}
		sig := &signalerMock{}
		sig.failedFunc = func() {}
		sig.canceledFunc = func() {
			t.Error("Unexpected canceled signal.")
		}
		sig.completedFunc = func() {
			t.Error("Unexpected completed signal.")
		}

		err = out.BulkPublish(sig, outputs.Options{}, data)
		if err == nil {
			t.Errorf("%+s: Failure does not propagate error", testCase)
		}
	}
}

func getDummyItem() outputs.Data {
	return outputs.Data{
		Event: common.MapStr(map[string]interface{}{
			"item":       "item",
			"dummy_data": "dummy_value",
		}),
	}
}
