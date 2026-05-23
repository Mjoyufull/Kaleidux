pub(crate) fn chroma_plane_extent(width: u32, height: u32) -> (u32, u32) {
    (width.div_ceil(2), height.div_ceil(2))
}

pub(crate) fn yuv420_aux_byte_size(width: u32, height: u32) -> u64 {
    let (chroma_width, chroma_height) = chroma_plane_extent(width, height);
    width as u64 * height as u64 + 2 * chroma_width as u64 * chroma_height as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chroma_plane_extent_rounds_up_for_odd_sizes() {
        assert_eq!(chroma_plane_extent(1920, 1080), (960, 540));
        assert_eq!(chroma_plane_extent(1921, 1081), (961, 541));
        assert_eq!(chroma_plane_extent(1919, 1079), (960, 540));
    }

    #[test]
    fn yuv420_aux_bytes_match_planar_layout() {
        let y = 1920u64 * 1080;
        let uv = 2 * 960u64 * 540;
        assert_eq!(yuv420_aux_byte_size(1920, 1080), y + uv);
        assert_eq!(
            yuv420_aux_byte_size(1919, 1079),
            1919 * 1079 + 2 * 960 * 540
        );
    }
}
