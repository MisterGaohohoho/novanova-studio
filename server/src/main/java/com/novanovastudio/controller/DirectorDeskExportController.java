package com.novanovastudio.controller;

import com.novanovastudio.common.ApiResponse;
import com.novanovastudio.common.BusinessException;
import com.novanovastudio.common.ErrorCode;
import com.novanovastudio.dto.DirectorDeskExportDtos;
import com.novanovastudio.dto.VideoCompositionDtos;
import com.novanovastudio.service.VideoCompositionTaskService;
import jakarta.validation.Valid;
import org.springframework.http.server.reactive.ServerHttpRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * 3D 导演台逐帧导出接口。
 *
 * @author   zhenglin.cn.cq@gmail.com
 * @date     2026-10-11 00:00
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/directorDesk")
@RequiredArgsConstructor
public class DirectorDeskExportController {

    /** 视频任务服务。 */
    private final VideoCompositionTaskService videoCompositionTaskService;

    /** 创建导演台导出准备会话。 */
    @PostMapping("/createExportSession")
    public Mono<ApiResponse<VideoCompositionDtos.VideoCompositionTaskResponse>> createExportSession(
            @Valid @RequestBody DirectorDeskExportDtos.CreateExportSessionRequest request, ServerHttpRequest httpRequest) {
        log.info("创建导演台导出会话: url=/api/v1/directorDesk/createExportSession, headers={}, request={}", httpRequest.getHeaders(), request);
        return videoCompositionTaskService.createDirectorDeskExportSession(request)
                .doOnNext(response -> log.info("导演台导出会话已创建: response={}", response))
                .map(ApiResponse::ok);
    }

    /** 上传导演台导出帧。 */
    @PostMapping("/uploadExportFrameBatch")
    public Mono<ApiResponse<VideoCompositionDtos.VideoCompositionTaskResponse>> uploadExportFrameBatch(
            @RequestPart("file") Mono<FilePart> file,
            @RequestPart("taskId") String taskId,
            @RequestPart("frameNumber") String frameNumber,
            @RequestPart("width") String width,
            @RequestPart("height") String height,
            @RequestPart("sha256") String sha256,
            ServerHttpRequest httpRequest) {
        DirectorDeskExportDtos.ExportFrameRequest request = new DirectorDeskExportDtos.ExportFrameRequest(taskId, parsePositiveInt(frameNumber, "帧序号"), parsePositiveInt(width, "帧宽度"), parsePositiveInt(height, "帧高度"), sha256);
        log.info("上传导演台导出帧: url=/api/v1/directorDesk/uploadExportFrameBatch, headers={}, taskId={}, frameNumber={}, width={}, height={}", httpRequest.getHeaders(), taskId, request.frameNumber(), request.width(), request.height());
        return file.flatMap(filePart -> videoCompositionTaskService.uploadDirectorDeskExportFrame(request, filePart))
                .doOnNext(response -> log.info("导演台导出帧已上传: response={}", response))
                .map(ApiResponse::ok);
    }

    /** 封存导演台导出帧并进入编码队列。 */
    @PostMapping("/finishExportFrames")
    public Mono<ApiResponse<VideoCompositionDtos.VideoCompositionTaskResponse>> finishExportFrames(
            @Valid @RequestBody DirectorDeskExportDtos.FinishExportFramesRequest request, ServerHttpRequest httpRequest) {
        log.info("封存导演台导出帧: url=/api/v1/directorDesk/finishExportFrames, headers={}, request={}", httpRequest.getHeaders(), request);
        return videoCompositionTaskService.finishDirectorDeskExportFrames(request)
                .doOnNext(response -> log.info("导演台导出帧已封存: response={}", response))
                .map(ApiResponse::ok);
    }

    /** 解析正整数表单字段。 */
    private int parsePositiveInt(String value, String label) {
        try {
            int result = Integer.parseInt(value);
            if (result > 0) return result;
        } catch (NumberFormatException ignored) {
            // 统一转换为中文参数错误。
        }
        throw new BusinessException(ErrorCode.PARAM_INVALID, label + "必须为正整数");
    }
}
