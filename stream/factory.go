package stream

import (
	"context"
	"fmt"
	"sync"

	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/RyanBlaney/latency-benchmark-common/stream/hls"
	"github.com/RyanBlaney/latency-benchmark-common/stream/icecast"
)

// Factory implements the StreamManager interface and provides a thread-safe
// registry for stream handler factories. It uses the factory pattern to create
// new handler instances on demand, ensuring each stream operation gets a fresh
// handler instance.
//
// The factory maintains a registry of handler factory functions rather than
// handler instances, which allows for:
// - Thread-safe handler creation
// - Fresh handler instances for each stream
// - Dynamic registration of new stream types
// - Proper resource isolation between concurrent operations
type Factory struct {
	handlers map[common.StreamType]func() common.StreamHandler
	detector common.StreamDetector
	mu       sync.RWMutex // Protects the handlers map for concurrent access
}

// NewFactory creates a new stream factory with default handlers for all
// supported stream types. The factory comes pre-configured with:
// - HLS handler factory
// - ICEcast handler factory
// - Default stream detector with standard timeout and redirect settings
//
// The factory is ready to use immediately and thread-safe for concurrent
// access across multiple goroutines.
//
// Example:
//
//	factory := stream.NewFactory()
//	handler, err := factory.CreateHandler(common.StreamTypeHLS)
//	if err != nil {
//	    // Handle creation error
//	}
//	defer handler.Close()
func NewFactory() *Factory {
	f := &Factory{
		handlers: make(map[common.StreamType]func() common.StreamHandler),
		detector: NewDetector(),
	}

	// Register default handlers
	f.RegisterHandlerFactory(common.StreamTypeHLS, func() common.StreamHandler {
		return hls.NewHandler()
	})

	f.RegisterHandlerFactory(common.StreamTypeICEcast, func() common.StreamHandler {
		return icecast.NewHandler()
	})

	return f
}

// CreateHandler creates a new handler instance for the specified stream type.
// Each call returns a fresh handler instance, ensuring proper isolation
// between different stream operations.
//
// This method is thread-safe and can be called concurrently from multiple
// goroutines without synchronization concerns.
//
// Parameters:
//   - streamType: The type of stream handler to create (HLS, ICEcast, etc.)
//
// Returns:
//   - StreamHandler: A new handler instance configured for the stream type
//   - error: Returns error if the stream type is not supported/registered
//
// Example:
//
//	factory := stream.NewFactory()
//	handler, err := factory.CreateHandler(common.StreamTypeHLS)
//	if err != nil {
//	    log.Printf("Unsupported stream type: %v", err)
//	    return
//	}
//	defer handler.Close()
//
//	err = handler.Connect(ctx, "https://example.com/playlist.m3u8")
func (f *Factory) CreateHandler(streamType common.StreamType) (common.StreamHandler, error) {
	f.mu.RLock()
	handlerFactory, exists := f.handlers[streamType]
	f.mu.RUnlock()

	if !exists {
		return nil, common.NewStreamError(
			streamType, "", common.ErrCodeUnsupported,
			fmt.Sprintf("unsupported stream type: %s", streamType),
			nil,
		)
	}

	return handlerFactory(), nil
}

// DetectAndCreate combines stream type detection with handler creation in a
// single operation. This is the most convenient method for working with
// streams when you don't know the type in advance.
//
// The method performs the following steps:
//  1. Uses the internal detector to identify the stream type
//  2. Creates an appropriate handler for the detected type
//  3. Returns the ready-to-use handler
//
// This method is particularly useful for applications that need to handle
// various stream types dynamically based on user input or configuration.
//
// Parameters:
//   - ctx: Context for timeout and cancellation control during detection
//   - url: The stream URL to analyze and create a handler for
//
// Returns:
//   - StreamHandler: A new handler instance appropriate for the detected stream type
//   - error: Returns error if detection fails or stream type is unsupported
//
// Example:
//
//	factory := stream.NewFactory()
//	handler, err := factory.DetectAndCreate(ctx, streamURL)
//	if err != nil {
//	    log.Printf("Failed to create handler: %v", err)
//	    return
//	}
//	defer handler.Close()
//
//	// Handler is already configured for the correct stream type
//	err = handler.Connect(ctx, streamURL)
func (f *Factory) DetectAndCreate(ctx context.Context, url string) (common.StreamHandler, error) {
	streamType, err := f.detector.DetectType(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to detect stream type: %w", err)
	}

	handler, err := f.CreateHandler(streamType)
	if err != nil {
		return nil, err
	}

	return handler, nil
}

// RegisterHandler registers a custom stream handler instance for a specific
// stream type. This method is deprecated in favor of RegisterHandlerFactory
// because it can lead to shared state issues when the same handler instance
// is used across multiple concurrent operations.
//
// Deprecated: Use RegisterHandlerFactory instead to ensure proper isolation
// between concurrent operations.
//
// Parameters:
//   - streamType: The stream type to register the handler for
//   - handler: The handler instance to register
//
// Returns:
//   - error: Always returns nil for compatibility
//
// Migration example:
//
//	// Old (deprecated):
//	factory.RegisterHandler(myType, myHandler)
//
//	// New (recommended):
//	factory.RegisterHandlerFactory(myType, func() common.StreamHandler {
//	    return NewMyHandler()
//	})
func (f *Factory) RegisterHandler(streamType common.StreamType, handler common.StreamHandler) error {
	return f.RegisterHandlerFactory(streamType, func() common.StreamHandler {
		return handler
	})
}

// RegisterHandlerFactory registers a factory function that creates new handler
// instances for a specific stream type. This is the preferred method for
// registering custom handlers as it ensures each operation gets a fresh
// handler instance.
//
// The factory function will be called each time CreateHandler or DetectAndCreate
// needs to create a handler of the specified type. This ensures proper isolation
// and thread safety.
//
// This method is thread-safe and can be called concurrently.
//
// Parameters:
//   - streamType: The stream type to register the factory for
//   - factory: Function that returns a new handler instance when called
//
// Returns:
//   - error: Always returns nil for interface compatibility
//
// Example:
//
//	// Register a custom stream type
//	factory.RegisterHandlerFactory(customType, func() common.StreamHandler {
//	    return &CustomHandler{
//	        config: customConfig,
//	        client: &http.Client{Timeout: 30 * time.Second},
//	    }
//	})
//
//	// Later, create instances as needed
//	handler, err := factory.CreateHandler(customType)
func (f *Factory) RegisterHandlerFactory(streamType common.StreamType, factory func() common.StreamHandler) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.handlers[streamType] = factory
	return nil
}

// SupportedTypes returns a slice of all currently supported stream types.
// This includes both built-in types (HLS, ICEcast) and any custom types
// that have been registered via RegisterHandlerFactory.
//
// The returned slice is a copy, so it's safe to modify without affecting
// the factory's internal state. The order of types in the slice is not
// guaranteed and may vary between calls.
//
// This method is thread-safe and can be called concurrently.
//
// Returns:
//   - []StreamType: Slice of all supported stream types
//
// Example:
//
//	factory := stream.NewFactory()
//	types := factory.SupportedTypes()
//	fmt.Printf("Supported types: %v\n", types)
//	// Output: Supported types: [hls icecast]
//
//	// Check if a specific type is supported
//	isSupported := false
//	for _, t := range types {
//	    if t == common.StreamTypeHLS {
//	        isSupported = true
//	        break
//	    }
//	}
func (f *Factory) SupportedTypes() []common.StreamType {
	f.mu.RLock()
	defer f.mu.RUnlock()

	types := make([]common.StreamType, 0, len(f.handlers))
	for streamType := range f.handlers {
		types = append(types, streamType)
	}

	return types
}
