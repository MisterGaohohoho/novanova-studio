package com.novanovastudio.dto;

import com.alibaba.fastjson2.JSONObject;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * 3D 导演台帧序列导出接口数据结构。
 *
 * @author   zhenglin.cn.cq@gmail.com
 * @date     2026-10-11 00:00
 */
public final class DirectorDeskExportDtos {

    /** 禁止实例化工具类。 */
    private DirectorDeskExportDtos() {
    }

    /** 创建导演台导出准备会话请求。 */
    public record CreateExportSessionRequest(
            @NotBlank(message = "business.canvasIDIsRequired") String canvasDocumentId,
            @NotBlank(message = "business.directorNodeIDIsRequired") String directorNodeId,
            @Min(value = 1, message = "business.projectRevisionMustBePositive") long projectRevision,
            @NotBlank(message = "business.cameraIDIsRequired") String cameraId,
            @Min(value = 2000, message = "business.exportDurationMustBeAtLeast2Seconds") long durationMilliseconds,
            @Min(value = 1, message = "business.exportWidthMustBePositive") int width,
            @Min(value = 1, message = "business.exportHeightMustBePositive") int height,
            @Min(value = 1, message = "business.exportFrameRateMustBePositive") int frameRate,
            @Min(value = 1, message = "business.exportFrameCountMustBePositive") int frameCount,
            @NotNull(message = "business.projectSnapshotIsRequired") JSONObject projectSnapshot) {
    }

    /** 导演台帧序列上传请求参数。 */
    public record ExportFrameRequest(
            @NotBlank(message = "business.taskIDIsRequired") String taskId,
            @Min(value = 1, message = "business.frameNumbersStartAt1") int frameNumber,
            @Min(value = 1, message = "business.frameWidthMustBePositive") int width,
            @Min(value = 1, message = "business.frameHeightMustBePositive") int height,
            @NotBlank(message = "business.frameDigestIsRequired") String sha256) {
    }

    /** 封存导演台帧序列请求。 */
    public record FinishExportFramesRequest(
            @NotBlank(message = "business.taskIDIsRequired") String taskId,
            @Min(value = 1, message = "business.frameCountMustBePositive") int frameCount) {
    }
}
