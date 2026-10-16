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
            @NotBlank(message = "画布ID不能为空") String canvasDocumentId,
            @NotBlank(message = "导演台节点ID不能为空") String directorNodeId,
            @Min(value = 1, message = "工程修订号必须为正整数") long projectRevision,
            @NotBlank(message = "机位ID不能为空") String cameraId,
            @Min(value = 2000, message = "导出时长不能少于2秒") long durationMilliseconds,
            @Min(value = 1, message = "导出宽度必须为正整数") int width,
            @Min(value = 1, message = "导出高度必须为正整数") int height,
            @Min(value = 1, message = "导出帧率必须为正整数") int frameRate,
            @Min(value = 1, message = "导出帧数必须为正整数") int frameCount,
            @NotNull(message = "工程快照不能为空") JSONObject projectSnapshot) {
    }

    /** 导演台帧序列上传请求参数。 */
    public record ExportFrameRequest(
            @NotBlank(message = "任务ID不能为空") String taskId,
            @Min(value = 1, message = "帧序号必须从1开始") int frameNumber,
            @Min(value = 1, message = "帧宽度必须为正整数") int width,
            @Min(value = 1, message = "帧高度必须为正整数") int height,
            @NotBlank(message = "帧摘要不能为空") String sha256) {
    }

    /** 封存导演台帧序列请求。 */
    public record FinishExportFramesRequest(
            @NotBlank(message = "任务ID不能为空") String taskId,
            @Min(value = 1, message = "帧数必须为正整数") int frameCount) {
    }
}
