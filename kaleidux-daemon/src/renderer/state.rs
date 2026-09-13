impl super::Renderer {
    pub fn set_content_type(&mut self, content_type: crate::queue::ContentType) {
        if self.valid_content_type == crate::queue::ContentType::Video
            && content_type != crate::queue::ContentType::Video
        {
            // Native steady-state video is held as YUV planes rather than a
            // `current_texture`. Preserve its last rendered frame in RGBA before
            // releasing those planes so the incoming image has a real transition
            // source instead of being treated like first content.
            self.snapshot_yuv_for_transition();
            self.release_video_backend_resources("leaving video content");
            self.last_video_source_size = None;
            self.last_video_presentation_size = None;
        }
        if content_type != crate::queue::ContentType::Video {
            self.presented_video_session_id = 0;
        }
        self.valid_content_type = content_type;
    }

    pub fn transition_just_completed(&self) -> bool {
        self.transition_just_completed
    }
}
