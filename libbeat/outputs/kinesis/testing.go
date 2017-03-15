package kinesis

import (
	"github.com/elastic/beats/libbeat/common/op"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/kinesis"

)

type signalerMock struct {
	op.Signaler

	completedFunc func()
	failedFunc func()
	canceledFunc func()
}

func (sig *signalerMock) Completed() {
	sig.completedFunc()
}

func (sig *signalerMock) Failed() {
	sig.failedFunc()
}

func (sig *signalerMock) Canceled() {
	sig.canceledFunc()
}

type kinesisClientMock struct {
	kinesisiface.KinesisAPI

	putRecordFunc func(*kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error)
	putRecordsFunc func(*kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

func (m *kinesisClientMock) PutRecord(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
	return m.putRecordFunc(input)
}

func (m *kinesisClientMock) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	return m.putRecordsFunc(input)
}
