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

var testICEcastSRCStreamURL = "http://stream1.skyviewnetworks.com:8010/MSNBC"
var testICEcastCDNStreamURL = "https://tunein.cdnstream1.com/3511_96.mp3"

func TestICEcastDetection(t *testing.T) {
	logger := logging.NewDefaultLogger().WithFields(logging.Fields{
		"test": "TestICEcastDetection",
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	factory := NewFactory()
	handler, err := factory.CreateHandler(common.StreamTypeICEcast)
	if err != nil {
		logger.Error(err, "Failed to create ICEcast stream handler")
	}
	defer handler.Close()
	assert.NotNil(t, handler)

	// Source Stream
	srcStreamType, err := factory.detector.DetectType(ctx, testICEcastSRCStreamURL)
	if err != nil {
		logger.Error(err, "Failed to detect ICEcast SOURCE stream")
	}
	assert.NotNil(t, srcStreamType)
	assert.Equal(t, common.StreamTypeICEcast, srcStreamType)

	srcStreamMetadata, err := factory.detector.ProbeStream(ctx, testICEcastSRCStreamURL)
	if err != nil {
		logger.Error(err, "Failed to probe ICEcast SOURCE stream ")
	}
	assert.Nil(t, srcStreamMetadata) // This one doesn't have headers haha
	// TODO: verify metadata. we log for now
	srcMetadataJSON, err := json.Marshal(srcStreamMetadata)
	if err != nil {
		logger.Error(err, "Failed to marshal ICEcast SOURCE stream metadata")
	}
	assert.NotNil(t, srcMetadataJSON)
	logger.Info(string(srcMetadataJSON))

	srcDetector, err := factory.DetectAndCreate(ctx, testICEcastSRCStreamURL)
	if err != nil {
		logger.Error(err, "Failed to detect and create ICEcast SOURCE stream")
	}
	assert.NotNil(t, srcDetector)

	assert.True(t, srcDetector.CanHandle(ctx, testICEcastSRCStreamURL))

	// CDN Stream
	cdnStreamType, err := factory.detector.DetectType(ctx, testICEcastCDNStreamURL)
	if err != nil {
		logger.Error(err, "Failed to detect ICEcast CDN stream")
	}
	assert.NotNil(t, srcStreamType)
	assert.Equal(t, common.StreamTypeICEcast, cdnStreamType)

	cdnStreamMetadata, err := factory.detector.ProbeStream(ctx, testICEcastCDNStreamURL)
	if err != nil {
		logger.Error(err, "Failed to probe ICEcast CDN stream ")
	}
	assert.NotNil(t, cdnStreamMetadata)
	// TODO: verify metadata. we log for now
	cdnMetadataJSON, err := json.Marshal(cdnStreamMetadata)
	if err != nil {
		logger.Error(err, "Failed to marshal ICEcast CDN stream metadata")
	}
	assert.NotNil(t, cdnMetadataJSON)
	logger.Info(string(cdnMetadataJSON))

	cdnDetector, err := factory.DetectAndCreate(ctx, testICEcastCDNStreamURL)
	if err != nil {
		logger.Error(err, "Failed to detect and create ICEcast CDN stream")
	}
	assert.NotNil(t, cdnDetector)

	assert.True(t, cdnDetector.CanHandle(ctx, testICEcastCDNStreamURL))
}
