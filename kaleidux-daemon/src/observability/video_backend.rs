#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VideoBackendMetricKind {
    AppsinkSession,
    AppsinkCallback,
    AppsinkFramePublished,
    AppsinkMailboxDropped,
    AppsinkPublishCapped,
    MpvSession,
    MpvCaptureAttempt,
    MpvFramePublished,
    MpvCaptureError,
}

impl VideoBackendMetricKind {
    pub fn as_index(self) -> usize {
        match self {
            Self::AppsinkSession => 0,
            Self::AppsinkCallback => 1,
            Self::AppsinkFramePublished => 2,
            Self::AppsinkMailboxDropped => 3,
            Self::AppsinkPublishCapped => 4,
            Self::MpvSession => 5,
            Self::MpvCaptureAttempt => 6,
            Self::MpvFramePublished => 7,
            Self::MpvCaptureError => 8,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::AppsinkSession => "appsink_sessions",
            Self::AppsinkCallback => "appsink_callbacks",
            Self::AppsinkFramePublished => "appsink_published",
            Self::AppsinkMailboxDropped => "appsink_mailbox_dropped",
            Self::AppsinkPublishCapped => "appsink_publish_capped",
            Self::MpvSession => "mpv_sessions",
            Self::MpvCaptureAttempt => "mpv_capture_attempts",
            Self::MpvFramePublished => "mpv_published",
            Self::MpvCaptureError => "mpv_capture_errors",
        }
    }
}

pub const VIDEO_BACKEND_METRIC_KIND_COUNT: usize = 9;
pub const VIDEO_BACKEND_METRIC_KINDS: [VideoBackendMetricKind; VIDEO_BACKEND_METRIC_KIND_COUNT] = [
    VideoBackendMetricKind::AppsinkSession,
    VideoBackendMetricKind::AppsinkCallback,
    VideoBackendMetricKind::AppsinkFramePublished,
    VideoBackendMetricKind::AppsinkMailboxDropped,
    VideoBackendMetricKind::AppsinkPublishCapped,
    VideoBackendMetricKind::MpvSession,
    VideoBackendMetricKind::MpvCaptureAttempt,
    VideoBackendMetricKind::MpvFramePublished,
    VideoBackendMetricKind::MpvCaptureError,
];
