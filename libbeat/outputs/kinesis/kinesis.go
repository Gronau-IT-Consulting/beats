package kinesis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/op"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	_ "github.com/elastic/beats/libbeat/outputs/codecs/json"
)

func init() {
	outputs.RegisterOutputPlugin("kinesis", New)
}

type output struct {
	beat    common.BeatInfo
	codec   outputs.Codec
	svc     kinesisiface.KinesisAPI
	config  *config
}

const maxBulkItems = 100

// New instantiates a new Kinesis output instance.
func New(beat common.BeatInfo, cfg *common.Config, _ int) (outputs.Outputer, error) {
	config := defaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, err
	}

	out := &output{beat: beat}
	if err := out.init(config); err != nil {
		return nil, err
	}
	return out, nil
}

func (out *output) init(config config) error {
	var err error

	awsConfig := &aws.Config{
		Region: aws.String(config.Region),
	}
	out.svc = kinesis.New(
		session.Must(session.NewSession(awsConfig)),
	)
	out.config = &config

	codec, err := outputs.CreateEncoder(config.Codec)
	if err != nil {
		return err
	}
	out.codec = codec

	return nil
}

// Implement Outputer
func (out *output) Close() error {
	return nil
}

func (out *output) PublishEvent(sig op.Signaler, opts outputs.Options, data outputs.Data) error {
	var serializedEvent []byte
	var err error

	serializedEvent, err = out.codec.Encode(data.Event)
	if err != nil {
		op.SigCompleted(sig)
		return err
	}

	params := &kinesis.PutRecordInput{
		Data:                      serializedEvent,          // Required
		PartitionKey:              aws.String(out.config.PartitionKey), // Required
		StreamName:                aws.String(out.config.StreamName),   // Required
	}
	_, err = out.svc.PutRecord(params)

	if err != nil {
		logp.Critical("Unable to write data to Kinesis: %s", err.Error())
		op.Sig(sig, err)
		return err
	}

	op.Sig(sig, err)
	return err
}

func (out *output) BulkPublish(sig op.Signaler, opts outputs.Options, data []outputs.Data) error {
	records := []*kinesis.PutRecordsRequestEntry{}
	count := 0
	for _, item := range data {
		serializedEvent, err := out.codec.Encode(item.Event)
		if err != nil {
			op.Sig(sig, err)
			return err
		}
		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:            serializedEvent,
			PartitionKey:    aws.String(out.config.PartitionKey), // Required
		})
		count ++
		if count >= maxBulkItems {
			count = 0
			err := out.putRecords(records)
			if err != nil {
				logp.Critical("Unable to write data to Kinesis: %s", err.Error())
				op.Sig(sig, err)
				return err
			}
			records = []*kinesis.PutRecordsRequestEntry{}
		}
	}
	if len(records) > 0 {
		err := out.putRecords(records)
		if err != nil {
			logp.Critical("Unable to write data to Kinesis: %s", err.Error())
			op.Sig(sig, err)
			return err
		}
	}
	op.Sig(sig, nil)

	return nil
}

func (out *output) putRecords(records []*kinesis.PutRecordsRequestEntry) error {
	params := &kinesis.PutRecordsInput{
		Records: records,
		StreamName: aws.String(out.config.StreamName),
	}
	_, err := out.svc.PutRecords(params)

	return err
}

func (out *output) PublishEvents(signal op.Signaler, opts outputs.Options, data []outputs.Data) error {
	return out.BulkPublish(signal, opts, data)
}

