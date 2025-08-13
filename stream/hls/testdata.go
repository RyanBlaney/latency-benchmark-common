package hls

// Test URLs for HLS streams used across all test files
var (
	// Source stream URL - NBC Universal live stream
	TestSRCStreamURL = "https://tni-drct-msnbc-int-jg89w.fast.nbcuni.com/live/master.m3u8"

	// CDN stream URL - TuneIn CDN stream
	TestCDNStreamURL = "https://tunein.cdnstream1.com/3511_96.aac/playlist.m3u8"

	// Additional test URLs for various scenarios
	TestValidHLSURLs = []string{
		"https://tunein.cdnstream1.com/3511_128.aac/playlist.m3u8",
		"https://tunein.cdnstream1.com/3511_64.aac/playlist.m3u8",
		"https://tunein.cdnstream1.com/3511_32.aac/playlist.m3u8",
		"https://tunein.cdnstream1.com/3511_192.aac/playlist.m3u8",
		"https://tunein.cdnstream1.com/3512_96.aac/playlist.m3u8",
		"https://tunein.cdnstream1.com/3513_96.aac/playlist.m3u8",
		"https://tunein.cdnstream1.com/3514_96.aac/playlist.m3u8",
		"https://tunein.cdnstream1.com/3511_96.mp3/playlist.m3u8",
		"https://tunein.cdnstream1.com/stream/3511_96.aac/playlist.m3u8",
	}

	// Invalid URLs for negative testing
	TestInvalidURLs = []string{
		"not-a-url",
		"ftp://example.com/file.m3u8",
		"https://example.com/file.mp3",
		"",
	}

	// Sample M3U8 content for testing
	TestM3U8MasterPlaylist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=1280000,CODECS="avc1.42e00a,mp4a.40.2",RESOLUTION=852x480
480p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2560000,CODECS="avc1.42e00a,mp4a.40.2",RESOLUTION=1280x720
720p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5000000,CODECS="avc1.42e00a,mp4a.40.2",RESOLUTION=1920x1080
1080p.m3u8`

	TestM3U8MediaPlaylist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:9.009,
segment0.ts
#EXTINF:9.009,
segment1.ts
#EXTINF:9.009,
segment2.ts
#EXT-X-ENDLIST`

	TestM3U8LivePlaylist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:123456
#EXTINF:10.0,
segment123456.ts
#EXTINF:10.0,
segment123457.ts
#EXTINF:10.0,
segment123458.ts`

	TestM3U8WithAdBreaks = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:9.009,Regular Content
segment0.ts
#EXT-X-CUE-OUT:30.0
#EXTINF:9.009,Ad Content
ad_segment1.ts
#EXT-X-CUE-IN
#EXTINF:9.009,Back to Content
segment1.ts
#EXT-X-ENDLIST`

	TestM3U8AudioOnly = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.0,CATEGORY:music
audio_segment0.aac
#EXTINF:10.0,CATEGORY:music
audio_segment1.aac
#EXTINF:10.0,CATEGORY:news
audio_segment2.aac
#EXT-X-ENDLIST`
)
