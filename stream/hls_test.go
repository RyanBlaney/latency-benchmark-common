package stream

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/stretchr/testify/assert"
)

var testHLSSRCStreamURL = "https://tni-drct-msnbc-int-jg89w.fast.nbcuni.com/live/master.m3u8"
var testHLSCDNStreamURL = "https://tunein.cdnstream1.com/3511_96.aac/playlist.m3u8"

func TestHLSDetection(t *testing.T) {
	logger := logging.NewDefaultLogger().WithFields(logging.Fields{
		"test": "TestHLSDetection",
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	factory := NewFactory()
	handler, err := factory.CreateHandler(common.StreamTypeHLS)
	if err != nil {
		logger.Error(err, "Failed to create HLS stream handler")
	}
	defer handler.Close()
	assert.NotNil(t, handler)

	// Source Stream
	srcStreamType, err := factory.detector.DetectType(ctx, testHLSSRCStreamURL)
	if err != nil {
		logger.Error(err, "Failed to detect HLS SOURCE stream")
	}
	assert.NotNil(t, srcStreamType)
	assert.Equal(t, common.StreamTypeHLS, srcStreamType)

	srcStreamMetadata, err := factory.detector.ProbeStream(ctx, testHLSSRCStreamURL)
	if err != nil {
		logger.Error(err, "Failed to probe HLS SOURCE stream ")
	}
	assert.NotNil(t, srcStreamMetadata)
	// TODO: verify metadata. we log for now
	srcMetadataJSON, err := json.Marshal(srcStreamMetadata)
	if err != nil {
		logger.Error(err, "Failed to marshal HLS SOURCE stream metadata")
	}
	assert.NotNil(t, srcMetadataJSON)
	logger.Info(string(srcMetadataJSON))

	srcDetector, err := factory.DetectAndCreate(ctx, testHLSSRCStreamURL)
	if err != nil {
		logger.Error(err, "Failed to detect and create HLS SOURCE stream")
	}
	assert.NotNil(t, srcDetector)

	assert.True(t, srcDetector.CanHandle(ctx, testHLSSRCStreamURL))

	// CDN Stream
	cdnStreamType, err := factory.detector.DetectType(ctx, testHLSCDNStreamURL)
	if err != nil {
		logger.Error(err, "Failed to detect HLS CDN stream")
	}
	assert.NotNil(t, srcStreamType)
	assert.Equal(t, common.StreamTypeHLS, cdnStreamType)

	cdnStreamMetadata, err := factory.detector.ProbeStream(ctx, testHLSCDNStreamURL)
	if err != nil {
		logger.Error(err, "Failed to probe HLS CDN stream ")
	}
	assert.NotNil(t, cdnStreamMetadata)
	// TODO: verify metadata. we log for now
	cdnMetadataJSON, err := json.Marshal(cdnStreamMetadata)
	if err != nil {
		logger.Error(err, "Failed to marshal HLS CDN stream metadata")
	}
	assert.NotNil(t, cdnMetadataJSON)
	logger.Info(string(cdnMetadataJSON))

	cdnDetector, err := factory.DetectAndCreate(ctx, testHLSCDNStreamURL)
	if err != nil {
		logger.Error(err, "Failed to detect and create HLS CDN stream")
	}
	assert.NotNil(t, cdnDetector)

	assert.True(t, cdnDetector.CanHandle(ctx, testHLSCDNStreamURL))
}
