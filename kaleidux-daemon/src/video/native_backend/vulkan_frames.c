#include <errno.h>
#include <libavcodec/avcodec.h>
#include <libavutil/hwcontext.h>
#ifdef KLD_VULKAN_HEADERS
#include <libavutil/hwcontext_vulkan.h>
#endif

/* Called only from get_format. FFmpeg negotiates codec/profile/DPB parameters;
 * keep those and request exportable modifier tiling before pool initialization.
 * On unsupported drivers leave the context unset for FFmpeg's normal pool. */
int kld_vulkan_export_frames(AVCodecContext *codec)
{
#ifdef KLD_VULKAN_HEADERS
    AVBufferRef *reference = NULL;
    int result = avcodec_get_hw_frames_parameters(codec, codec->hw_device_ctx,
                                                 AV_PIX_FMT_VULKAN, &reference);
    if (result < 0)
        return result;
    AVHWFramesContext *frames = (AVHWFramesContext *)reference->data;
    AVVulkanFramesContext *vulkan = frames->hwctx;
    vulkan->tiling = VK_IMAGE_TILING_DRM_FORMAT_MODIFIER_EXT;
    result = av_hwframe_ctx_init(reference);
    if (result < 0) {
        av_buffer_unref(&reference);
        return result;
    }
    av_buffer_unref(&codec->hw_frames_ctx);
    codec->hw_frames_ctx = reference;
    return 0;
#else
    (void)codec;
    return AVERROR(ENOSYS);
#endif
}
