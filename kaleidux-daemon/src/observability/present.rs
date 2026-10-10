#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RendererPresentKind {
    StaticImage,
    Transition,
    AppsinkVideo,
    StartupRelease,
    Black,
    ResizeReconfigure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameCallbackKind {
    StaticImage,
    Transition,
    AppsinkVideo,
    Other,
}

impl FrameCallbackKind {
    pub fn as_index(self) -> usize {
        match self {
            Self::StaticImage => 0,
            Self::Transition => 1,
            Self::AppsinkVideo => 2,
            Self::Other => 3,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::StaticImage => "static_callbacks",
            Self::Transition => "transition_callbacks",
            Self::AppsinkVideo => "appsink_video_callbacks",
            Self::Other => "other_callbacks",
        }
    }
}

impl RendererPresentKind {
    pub fn as_index(self) -> usize {
        match self {
            Self::StaticImage => 0,
            Self::Transition => 1,
            Self::AppsinkVideo => 2,
            Self::StartupRelease => 3,
            Self::Black => 4,
            Self::ResizeReconfigure => 5,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::StaticImage => "static",
            Self::Transition => "transition",
            Self::AppsinkVideo => "appsink_video",
            Self::StartupRelease => "startup_release",
            Self::Black => "black",
            Self::ResizeReconfigure => "resize_reconfigure",
        }
    }
}

pub const RENDERER_PRESENT_KIND_COUNT: usize = 6;
pub const RENDERER_PRESENT_KINDS: [RendererPresentKind; RENDERER_PRESENT_KIND_COUNT] = [
    RendererPresentKind::StaticImage,
    RendererPresentKind::Transition,
    RendererPresentKind::AppsinkVideo,
    RendererPresentKind::StartupRelease,
    RendererPresentKind::Black,
    RendererPresentKind::ResizeReconfigure,
];

pub const FRAME_CALLBACK_KIND_COUNT: usize = 4;
pub const FRAME_CALLBACK_KINDS: [FrameCallbackKind; FRAME_CALLBACK_KIND_COUNT] = [
    FrameCallbackKind::StaticImage,
    FrameCallbackKind::Transition,
    FrameCallbackKind::AppsinkVideo,
    FrameCallbackKind::Other,
];
