package com.novanovastudio.service;

import com.novanovastudio.common.SystemMessage;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.novanovastudio.ai.AiTaskPollingSupport;
import com.novanovastudio.common.BusinessException;
import com.novanovastudio.common.ErrorCode;
import com.novanovastudio.config.NovanovaProperties;
import com.novanovastudio.dto.PersistenceDtos;
import com.novanovastudio.dto.DirectorDeskExportDtos;
import com.novanovastudio.dto.VideoCompositionDtos;
import com.novanovastudio.entity.VideoCompositionTask;
import com.novanovastudio.repository.VideoCompositionTaskRepository;
import com.novanovastudio.security.CurrentUserProvider;
import com.novanovastudio.task.VideoCompositionMediaProcessor;
import com.novanovastudio.task.VideoCompositionTaskCancellation;
import com.novanovastudio.task.VideoCompositionTaskDispatcher;
import com.novanovastudio.task.VideoCompositionTaskQueue;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * 画布视频合成任务业务服务。
 *
 * @author   zhenglin.cn.cq@gmail.com
 * @date     2026-08-11 00:00
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class VideoCompositionTaskService {

    /** 等待状态 */
    private static final String STATUS_PENDING = "pending";

    /** 执行中状态 */
    private static final String STATUS_RUNNING = "running";

    /** 成功状态 */
    private static final String STATUS_SUCCEEDED = "succeeded";

    /** 失败状态 */
    private static final String STATUS_FAILED = "failed";

    /** 已取消状态 */
    private static final String STATUS_CANCELED = "canceled";

    /** 普通视频拼接任务类型 */
    static final String TASK_TYPE_VIDEO_COMPOSITION = "videoComposition";

    /** 导演台帧序列导出任务类型 */
    static final String TASK_TYPE_DIRECTOR_DESK_EXPORT = "directorDeskExport";

    /** 最大合成视频数量 */
    private static final int MAXIMUM_SOURCE_VIDEO_COUNT = 20;

    /** 导演台固定导出帧率。 */
    private static final int DIRECTOR_DESK_FRAME_RATE = 30;

    /** 导演台最短导出时长。 */
    private static final long DIRECTOR_DESK_MINIMUM_DURATION_MILLISECONDS = 2000L;

    /** 导演台最长导出时长。 */
    private static final long DIRECTOR_DESK_MAXIMUM_DURATION_MILLISECONDS = 15000L;

    /** 导演台单帧 PNG 的最大允许大小。 */
    private static final long DIRECTOR_DESK_FRAME_MAXIMUM_BYTES = 12L * 1024 * 1024;

    /** 导演台准备会话最长保留时间。 */
    private static final Duration DIRECTOR_DESK_PREPARING_TIMEOUT = Duration.ofMinutes(30);

    /** 任务仓储 */
    private final VideoCompositionTaskRepository repository;

    /** 当前用户提供器 */
    private final CurrentUserProvider currentUserProvider;

    /** 媒体持久化服务 */
    private final PersistenceService persistenceService;

    /** 视频媒体处理器 */
    private final VideoCompositionMediaProcessor mediaProcessor;

    /** 视频合成任务调度器 */
    private final VideoCompositionTaskDispatcher taskDispatcher;

    /** 视频合成任务队列 */
    private final VideoCompositionTaskQueue taskQueue;

    /** 视频合成任务取消标记 */
    private final VideoCompositionTaskCancellation cancellation;

    /** 服务配置 */
    private final NovanovaProperties properties;

    /** 未完成任务恢复订阅 */
    private Disposable recoveryDisposable;

    /** 当前实例正在接收的导演台帧上传数量。 */
    private final AtomicInteger directorDeskUploadCount = new AtomicInteger();

    /**
     * 服务启动时恢复未完成的视频合成任务。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void recoverUnfinishedTasks() {
        if (recoveryDisposable != null) {
            recoveryDisposable.dispose();
        }
        Duration pollingInterval = AiTaskPollingSupport.pollingInterval(properties);
        recoveryDisposable = Flux.interval(Duration.ZERO, pollingInterval)
                .concatMap(ignored -> recoverUnfinishedTasksOnce().onErrorResume(exception -> {
                    log.error("恢复视频合成任务失败", exception);
                    return Mono.<Void>empty();
                }))
                .subscribe(
                        ignored -> {
                        },
                        exception -> log.error("视频合成任务恢复订阅异常终止", exception)
                );
    }

    /**
     * 停止未完成任务恢复订阅。
     */
    @PreDestroy
    public void stopRecovery() {
        if (recoveryDisposable != null) {
            recoveryDisposable.dispose();
        }
    }

    /**
     * 将等待任务以及已失去活动租约的运行中任务重新写入队列。
     *
     * @return Mono<Void> 本轮恢复结果
     */
    private Mono<Void> recoverUnfinishedTasksOnce() {
        OffsetDateTime preparingCutoff = OffsetDateTime.now(ZoneOffset.UTC).minus(DIRECTOR_DESK_PREPARING_TIMEOUT);
        return repository.listExpiredDirectorDeskPreparingTasks(preparingCutoff)
                .concatMap(task -> repository.cancelTask(task.getUserId(), task.getId())
                        .then(cleanupDirectorFrames(task)))
                .thenMany(repository.listPendingTasks()
                        .concatMap(task -> taskDispatcher.enqueue(task.getId()))
                        .thenMany(repository.listRunningTasks()
                                .concatMap(task -> taskQueue.hasActiveTask(task.getId())
                                        .filter(active -> !active)
                                        .flatMap(ignored -> repository.requeueInactiveRunningTask(task.getId()))
                                        .filter(Boolean::booleanValue)
                                        .flatMap(ignored -> taskDispatcher.enqueue(task.getId())))))
                .then();
    }

    /**
     * 创建当前用户的视频合成任务。
     *
     * @param request CreateVideoCompositionRequest 创建请求
     * @return Mono<VideoCompositionTaskResponse> 已创建任务响应
     */
    public Mono<VideoCompositionDtos.VideoCompositionTaskResponse> createTask(
            VideoCompositionDtos.CreateVideoCompositionRequest request) {
        List<String> sourceStorageKeys = normalizeSourceStorageKeys(request == null ? null : request.sourceStorageKeys());
        return currentUserProvider.currentUserId().flatMap(userId -> Flux.fromIterable(sourceStorageKeys)
                .concatMap(storageKey -> persistenceService.validateVideoMediaForUser(userId, storageKey))
                .then(Mono.defer(() -> {
                    VideoCompositionTask task = new VideoCompositionTask();
                    task.setId(UUID.randomUUID().toString());
                    task.setUserId(userId);
                    task.setTaskType(TASK_TYPE_VIDEO_COMPOSITION);
                    task.setStatus(STATUS_PENDING);
                    task.setProgress(0);
                    task.setSourceStorageKeys(JSON.toJSONString(sourceStorageKeys));
                    task.setResultData("{}");
                    task.setErrorMessage("");
                    log.info("创建视频合成任务: taskId={}, userId={}, sourceCount={}", task.getId(), userId, sourceStorageKeys.size());
                    return cancellation.clearCancellation(task.getId())
                            .then(repository.createTask(task))
                            .then(taskDispatcher.enqueue(task.getId()))
                            .thenReturn(taskResponse(task));
                })));
    }

    /**
     * 查询当前用户的视频合成任务。
     *
     * @param task VideoCompositionTask 运行中的任务
     * @return Mono<VideoCompositionTaskResponse> 任务响应
     */
    public Mono<VideoCompositionDtos.VideoCompositionTaskResponse> getTask(String taskId) {
        return currentUserProvider.currentUserId()
                .flatMap(userId -> repository.getTask(userId, taskId)
                        .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.RESOURCE_NOT_FOUND, SystemMessage.of("business.videoCompositionTaskNotFound"))))
                        .map(this::taskResponse));
    }

    /**
     * 取消当前用户的视频合成任务。
     *
     * @param taskId String 任务ID
     * @return Mono<VideoCompositionTaskResponse> 已取消或原任务响应
     */
    public Mono<VideoCompositionDtos.VideoCompositionTaskResponse> cancelTask(String taskId) {
        return currentUserProvider.currentUserId().flatMap(userId -> repository.getTask(userId, taskId)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.RESOURCE_NOT_FOUND, SystemMessage.of("business.videoCompositionTaskNotFound"))))
                .flatMap(task -> {
                    if (isTerminal(task.getStatus())) {
                        return Mono.just(taskResponse(task));
                    }
                    return cancellation.requestCancellation(taskId)
                            .then(repository.cancelTask(userId, taskId))
                            .then(cleanupDirectorFrames(task))
                            .then(repository.getTask(userId, taskId))
                            .map(this::taskResponse);
                }));
    }

    /**
     * 创建导演台逐帧导出准备会话。
     *
     * @param request CreateExportSessionRequest 导出参数与工程快照
     * @return Mono<VideoCompositionTaskResponse> 准备态任务
     */
    public Mono<VideoCompositionDtos.VideoCompositionTaskResponse> createDirectorDeskExportSession(
            DirectorDeskExportDtos.CreateExportSessionRequest request) {
        validateDirectorDeskExportRequest(request);
        return currentUserProvider.currentUserId().flatMap(userId -> repository.countDirectorDeskPreparingTasks(userId)
                .flatMap(count -> {
                    int maximumPreparingTasks = Math.max(1, properties.getAi().getVideoComposition().getDirectorDeskMaximumPreparingTasks());
                    if (count >= maximumPreparingTasks) {
                        return Mono.error(new BusinessException(ErrorCode.BUSINESS_ERROR, SystemMessage.of("business.directorExportPreparationLimitReachedFinishOrCancelAnExisting")));
                    }
                    return persistenceService.getProject(request.canvasDocumentId());
                })
                .flatMap(project -> validateDirectorDeskProjectSnapshot(project, request))
                .then(Mono.defer(() -> {
                    VideoCompositionTask task = new VideoCompositionTask();
                    task.setId(UUID.randomUUID().toString());
                    task.setUserId(userId);
                    task.setTaskType(TASK_TYPE_DIRECTOR_DESK_EXPORT);
                    task.setStatus("preparing");
                    task.setProgress(0);
                    JSONObject snapshot = new JSONObject();
                    snapshot.put("kind", TASK_TYPE_DIRECTOR_DESK_EXPORT);
                    snapshot.put("canvasDocumentId", request.canvasDocumentId());
                    snapshot.put("directorNodeId", request.directorNodeId());
                    snapshot.put("projectRevision", request.projectRevision());
                    snapshot.put("cameraId", request.cameraId());
                    snapshot.put("width", request.width());
                    snapshot.put("height", request.height());
                    snapshot.put("frameRate", request.frameRate());
                    snapshot.put("durationMilliseconds", request.durationMilliseconds());
                    snapshot.put("frameCount", request.frameCount());
                    snapshot.put("project", request.projectSnapshot());
                    snapshot.put("frames", new JSONArray());
                    task.setSourceStorageKeys(snapshot.toJSONString());
                    task.setResultData("{}");
                    task.setErrorMessage("");
                    log.info("创建导演台导出准备任务: taskId={}, userId={}, frameCount={}", task.getId(), userId, request.frameCount());
                    return cancellation.clearCancellation(task.getId())
                            .then(repository.createTask(task))
                            .thenReturn(taskResponse(task));
                })));
    }

    /**
     * 接收导演台单帧 PNG，服务端保存后追加到不可变工程快照的帧清单。
     *
     * @param request ExportFrameRequest 帧序号与尺寸
     * @param file FilePart PNG文件
     * @return Mono<VideoCompositionTaskResponse> 更新后的准备任务
     */
    public Mono<VideoCompositionDtos.VideoCompositionTaskResponse> uploadDirectorDeskExportFrame(
            DirectorDeskExportDtos.ExportFrameRequest request, FilePart file) {
        return withDirectorDeskUploadSlot(() -> currentUserProvider.currentUserId()
                .flatMap(userId -> repository.getTask(userId, request.taskId())
                        .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.RESOURCE_NOT_FOUND, SystemMessage.of("business.directorExportTaskNotFound"))))
                        .flatMap(task -> {
                            JSONObject snapshot = parseDirectorSnapshot(task);
                            ensureDirectorPreparingTask(task, snapshot, request.frameNumber(), request.width(), request.height(), request.sha256());
                            JSONObject existing = findFrame(snapshot.getJSONArray("frames"), request.frameNumber());
                            if (existing != null) {
                                if (!request.sha256().equalsIgnoreCase(existing.getString("sha256"))) {
                                    throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.digestMismatchForTheSameFrameNumber"));
                                }
                                return Mono.just(taskResponse(task));
                            }
                            String normalizedSha256 = request.sha256().toLowerCase();
                            String storageKey = "director-frame:" + task.getId() + ":" + request.frameNumber() + ":" + normalizedSha256;
                            JSONObject metadata = new JSONObject();
                            metadata.put("sha256", normalizedSha256);
                            metadata.put("maximumBytes", DIRECTOR_DESK_FRAME_MAXIMUM_BYTES);
                            PersistenceDtos.RegisterRemoteMediaRequest mediaRequest = new PersistenceDtos.RegisterRemoteMediaRequest(
                                    "directorFrame", null, storageKey, "image/png", null, request.width(), request.height(), null, metadata);
                            return persistenceService.uploadMedia(file, mediaRequest)
                                    .flatMap(media -> appendDirectorDeskFrame(userId, task, request, normalizedSha256, media));
                        })));
    }

    /** 原子追加上传帧，并处理并发重复帧与任务状态变化。 */
    private Mono<VideoCompositionDtos.VideoCompositionTaskResponse> appendDirectorDeskFrame(
            Long userId, VideoCompositionTask task, DirectorDeskExportDtos.ExportFrameRequest request,
            String normalizedSha256, PersistenceDtos.UploadedMediaResponse media) {
        JSONObject frame = new JSONObject();
        frame.put("frameNumber", request.frameNumber());
        frame.put("storageKey", media.storageKey());
        frame.put("width", request.width());
        frame.put("height", request.height());
        frame.put("bytes", media.bytes());
        frame.put("sha256", normalizedSha256);
        long frameBytes = media.bytes() == null ? 0L : media.bytes();
        return repository.appendPreparingDirectorDeskFrame(
                        userId,
                        task.getId(),
                        request.frameNumber(),
                        frame.toJSONString(),
                        frameBytes,
                        properties.getAi().getVideoComposition().getDirectorDeskMaximumFrameBytes())
                .flatMap(updatedSnapshot -> repository.getTask(userId, task.getId()).map(this::taskResponse))
                .switchIfEmpty(Mono.defer(() -> repository.getTask(userId, task.getId())
                        .flatMap(latest -> {
                            JSONObject latestSnapshot = parseDirectorSnapshot(latest);
                            JSONObject latestFrame = findFrame(latestSnapshot.getJSONArray("frames"), request.frameNumber());
                            if (latestFrame != null && normalizedSha256.equalsIgnoreCase(latestFrame.getString("sha256"))) {
                                return Mono.just(taskResponse(latest));
                            }
                            Mono<Void> cleanup = media.storageKey().equals(latestFrame == null ? null : latestFrame.getString("storageKey"))
                                    ? Mono.empty()
                                    : persistenceService.deleteMediaForUser(userId, List.of(media.storageKey()));
                            SystemMessage message = SystemMessage.of(latestFrame == null ? "business.directorExportIsNoLongerPreparingOrItsFrameCapacity" : "business.digestMismatchForTheSameFrameNumber");
                            return cleanup.then(Mono.error(new BusinessException(ErrorCode.BUSINESS_ERROR, message)));
                        })
                        .switchIfEmpty(persistenceService.deleteMediaForUser(userId, List.of(media.storageKey()))
                                .then(Mono.error(new BusinessException(ErrorCode.RESOURCE_NOT_FOUND, SystemMessage.of("business.directorExportTaskNotFound")))))));
    }

    /**
     * 校验帧清单并把导演台准备任务转入现有视频队列。
     *
     * @param request FinishExportFramesRequest 封存请求
     * @return Mono<VideoCompositionTaskResponse> 已封存任务
     */
    public Mono<VideoCompositionDtos.VideoCompositionTaskResponse> finishDirectorDeskExportFrames(
            DirectorDeskExportDtos.FinishExportFramesRequest request) {
        return currentUserProvider.currentUserId().flatMap(userId -> repository.getTask(userId, request.taskId())
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.RESOURCE_NOT_FOUND, SystemMessage.of("business.directorExportTaskNotFound"))))
                .flatMap(task -> {
                    if (isTerminal(task.getStatus()) || !"preparing".equals(task.getStatus())) return Mono.just(taskResponse(task));
                    JSONObject snapshot = parseDirectorSnapshot(task);
                    validateDirectorFrameManifest(snapshot, request.frameCount());
                    return repository.sealDirectorDeskTask(userId, task.getId(), snapshot.toJSONString())
                            .flatMap(sealed -> Boolean.TRUE.equals(sealed)
                                    ? taskDispatcher.enqueue(task.getId()).then(repository.getTask(userId, task.getId()).map(this::taskResponse))
                                    : repository.getTask(userId, task.getId()).map(this::taskResponse));
                }));
    }

    /**
     * 执行队列中已领取的视频合成任务。
     *
     * @param taskId String 任务ID
     * @return Mono<Void> 执行结果
     */
    public Mono<Void> executeQueuedTask(String taskId) {
        return repository.getTaskById(taskId)
                .switchIfEmpty(Mono.fromRunnable(() -> log.info("跳过视频合成任务执行: taskId={}, reason={}", taskId, "任务不存在")).then(Mono.empty()))
                .flatMap(this::executeTask)
                .onErrorResume(exception -> {
                    log.error("执行视频合成任务发生未处理异常: taskId={}", taskId, exception);
                    return Mono.empty();
                });
    }

    /**
     * 根据当前任务状态执行合成流程。
     *
     * @param task VideoCompositionTask 任务快照
     * @return Mono<Void> 执行结果
     */
    private Mono<Void> executeTask(VideoCompositionTask task) {
        if (!STATUS_PENDING.equals(task.getStatus())) {
            log.info("跳过视频合成任务执行: taskId={}, status={}", task.getId(), task.getStatus());
            return Mono.empty();
        }
        return cancellation.isCancellationRequested(task.getId())
                .flatMap(cancelRequested -> {
                    if (Boolean.TRUE.equals(cancelRequested)) {
                        return repository.cancelTask(task.getUserId(), task.getId()).then(cancellation.clearCancellation(task.getId()));
                    }
                    return repository.markTaskRunning(task.getId())
                            .flatMap(started -> Boolean.TRUE.equals(started)
                                    ? TASK_TYPE_DIRECTOR_DESK_EXPORT.equals(task.getTaskType()) ? composeDirectorTask(task) : composeTask(task)
                                    : Mono.empty());
                });
    }

    /** 执行导演台封存帧序列的下载、编码和正式媒体登记。 */
    private Mono<Void> composeDirectorTask(VideoCompositionTask task) {
        JSONObject snapshot = parseDirectorSnapshot(task);
        AtomicInteger lastProgress = new AtomicInteger(0);
        JSONArray frames = snapshot.getJSONArray("frames");
        List<JSONObject> manifest = frames == null ? List.of() : frames.stream()
                .map(item -> item instanceof JSONObject value ? value : null)
                .filter(item -> item != null)
                .sorted(Comparator.comparingInt(item -> item.getIntValue("frameNumber")))
                .toList();
        return Mono.usingWhen(
                        createWorkingDirectory(task.getId()),
                        directory -> Flux.fromIterable(manifest)
                                .concatMap(frame -> {
                                    Path target = directory.resolve(String.format("frame-%06d.png", frame.getIntValue("frameNumber")));
                                    return persistenceService.downloadMediaToFileForUser(task.getUserId(), frame.getString("storageKey"), target);
                                })
                                .collectList()
                                .flatMap(downloaded -> Mono.fromCallable(() -> {
                                    Path outputFile = directory.resolve("director-desk-export.mp4");
                                    VideoCompositionMediaProcessor.CompositionResult result = mediaProcessor.encodeFrames(
                                            downloaded.stream().map(PersistenceService.DownloadedMediaFile::path).toList(), outputFile,
                                            snapshot.getIntValue("width"), snapshot.getIntValue("height"), snapshot.getIntValue("frameRate"),
                                            progress -> updateProgress(task.getId(), progress, lastProgress),
                                            () -> isCancellationRequested(task.getId()));
                                    return new CompositionOutput(outputFile, result);
                                }).subscribeOn(Schedulers.boundedElastic()))
                                .flatMap(output -> cancellation.isCancellationRequested(task.getId())
                                        .flatMap(cancelRequested -> Boolean.TRUE.equals(cancelRequested)
                                                ? Mono.error(new VideoCompositionMediaProcessor.VideoCompositionCanceledException())
                                                : persistenceService.storeGeneratedMediaFileForUser(task.getUserId(), "video", "director-desk-export.mp4", "video/mp4", output.path(), output.result().width(), output.result().height(), durationAsInteger(output.result().durationMilliseconds())))),
                        this::deleteWorkingDirectory,
                        (directory, exception) -> deleteWorkingDirectory(directory),
                        this::deleteWorkingDirectory
                )
                .flatMap(media -> finishSucceeded(task, media).then(cleanupDirectorFrames(task)))
                .onErrorResume(exception -> finishFailed(task, exception))
                .doFinally(signalType -> cancellation.clearCancellation(task.getId()).subscribe(
                        ignored -> {
                        },
                        exception -> log.error("清理导演台导出取消标记失败: taskId={}", task.getId(), exception)
                ));
    }

    /**
     * 执行媒体下载、FFmpeg合成和对象存储上传。
     *
     * @param task VideoCompositionTask 运行中的任务
     * @return Mono<Void> 合成结果
     */
    private Mono<Void> composeTask(VideoCompositionTask task) {
        List<String> sourceStorageKeys = parseSourceStorageKeys(task.getSourceStorageKeys());
        return Mono.usingWhen(
                        createWorkingDirectory(task.getId()),
                        directory -> composeInWorkingDirectory(task, sourceStorageKeys, directory),
                        this::deleteWorkingDirectory,
                        (directory, exception) -> deleteWorkingDirectory(directory),
                        this::deleteWorkingDirectory
                )
                .flatMap(media -> finishSucceeded(task, media))
                .onErrorResume(exception -> finishFailed(task, exception))
                .doFinally(signalType -> cancellation.clearCancellation(task.getId()).subscribe(
                        ignored -> {
                        },
                        exception -> log.error("清理视频合成取消标记失败: taskId={}", task.getId(), exception)
                ));
    }

    /**
     * 在任务私有临时目录中完成视频合成。
     *
     * @param task VideoCompositionTask 运行中的任务
     * @param sourceStorageKeys List<String> 源视频媒体存储键
     * @param directory Path 临时目录
     * @return Mono<UploadedMediaResponse> 已上传成片媒体
     */
    private Mono<PersistenceDtos.UploadedMediaResponse> composeInWorkingDirectory(
            VideoCompositionTask task, List<String> sourceStorageKeys, Path directory) {
        AtomicInteger lastProgress = new AtomicInteger(0);
        return Flux.fromIterable(sourceStorageKeys)
                .index()
                .concatMap(indexed -> cancellation.isCancellationRequested(task.getId())
                        .flatMap(cancelRequested -> {
                            if (Boolean.TRUE.equals(cancelRequested)) {
                                return Mono.error(new VideoCompositionMediaProcessor.VideoCompositionCanceledException());
                            }
                            Path target = directory.resolve("source-" + indexed.getT1() + ".media");
                            return persistenceService.downloadVideoMediaToFileForUser(task.getUserId(), indexed.getT2(), target);
                        }))
                .collectList()
                .flatMap(sourceFiles -> Mono.fromCallable(() -> {
                    Path outputFile = directory.resolve("composed-video.mp4");
                    VideoCompositionMediaProcessor.CompositionResult result = mediaProcessor.compose(
                            sourceFiles.stream().map(PersistenceService.DownloadedMediaFile::path).toList(),
                            outputFile,
                            progress -> updateProgress(task.getId(), progress, lastProgress),
                            () -> isCancellationRequested(task.getId())
                    );
                    return new CompositionOutput(outputFile, result);
                }).subscribeOn(Schedulers.boundedElastic()))
                .flatMap(output -> cancellation.isCancellationRequested(task.getId())
                        .flatMap(cancelRequested -> {
                            if (Boolean.TRUE.equals(cancelRequested)) {
                                return Mono.error(new VideoCompositionMediaProcessor.VideoCompositionCanceledException());
                            }
                            return persistenceService.storeGeneratedMediaFileForUser(
                                    task.getUserId(), "video", "canvas-video-composition.mp4", "video/mp4", output.path(),
                                    output.result().width(), output.result().height(), durationAsInteger(output.result().durationMilliseconds())
                            );
                        }));
    }

    /**
     * 创建任务私有临时目录。
     *
     * @param task VideoCompositionTask 运行中的任务
     * @return Mono<Path> 临时目录
     */
    private Mono<Path> createWorkingDirectory(String taskId) {
        String normalizedTaskId = taskId.replaceAll("[^a-zA-Z0-9-]", "");
        return Mono.fromCallable(() -> Files.createTempDirectory("novanova-video-composition-" + normalizedTaskId + "-"))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 删除任务临时目录。
     *
     * @param directory Path 临时目录
     * @return Mono<Void> 删除结果
     */
    private Mono<Void> deleteWorkingDirectory(Path directory) {
        return Mono.fromRunnable(() -> {
            try (var paths = Files.walk(directory)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException exception) {
                        throw new UncheckedIOException(exception);
                    }
                });
            } catch (IOException | UncheckedIOException exception) {
                log.error("清理视频合成临时目录失败: directory={}", directory, exception);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    /**
     * 回写任务进度，避免重复写入相同进度。
     *
     * @param taskId String 任务ID
     * @param progress int 最新进度
     * @param lastProgress AtomicInteger 已写入进度
     */
    private void updateProgress(String taskId, int progress, AtomicInteger lastProgress) {
        int normalized = Math.max(1, Math.min(99, progress));
        if (normalized <= lastProgress.getAndAccumulate(normalized, Math::max)) {
            return;
        }
        repository.updateProgress(taskId, normalized)
                .onErrorResume(exception -> {
                    log.error("更新视频合成任务进度失败: taskId={}, progress={}", taskId, normalized, exception);
                    return Mono.empty();
                })
                .block();
    }

    /**
     * 查询任务是否已请求取消。
     *
     * @param taskId String 任务ID
     * @return boolean 是否已取消
     */
    private boolean isCancellationRequested(String taskId) {
        return Boolean.TRUE.equals(cancellation.isCancellationRequested(taskId)
                .onErrorReturn(false)
                .block());
    }

    /**
     * 将成功结果写入任务。
     *
     * @param task VideoCompositionTask 运行中的任务
     * @param media UploadedMediaResponse 成片媒体信息
     * @return Mono<Void> 更新结果
     */
    private Mono<Void> finishSucceeded(VideoCompositionTask task, PersistenceDtos.UploadedMediaResponse media) {
        JSONObject result = new JSONObject();
        result.put("storageKey", media.storageKey());
        result.put("url", media.url());
        result.put("bytes", media.bytes());
        result.put("mimeType", media.mimeType());
        result.put("width", media.width());
        result.put("height", media.height());
        result.put("durationMs", media.durationMs());
        result.put("objectStorage", JSON.toJSON(media.objectStorage()));
        if (TASK_TYPE_DIRECTOR_DESK_EXPORT.equals(task.getTaskType())) {
            JSONObject snapshot = parseDirectorSnapshot(task);
            result.put("sourceCanvasDocumentId", snapshot.getString("canvasDocumentId"));
            result.put("sourceDirectorNodeId", snapshot.getString("directorNodeId"));
            result.put("sourceProjectRevision", snapshot.getLongValue("projectRevision"));
            result.put("sourceCameraId", snapshot.getString("cameraId"));
            result.put("sourceDurationMilliseconds", snapshot.getLongValue("durationMilliseconds"));
            result.put("sourceFrameRate", snapshot.getIntValue("frameRate"));
        }
        return cancellation.isCancellationRequested(task.getId())
                .onErrorReturn(false)
                .flatMap(cancelRequested -> {
                    if (Boolean.TRUE.equals(cancelRequested)) {
                        return repository.finishRunningTask(task.getId(), STATUS_CANCELED, "{}", SystemMessage.of("system.taskCanceled"))
                                .then(persistenceService.deleteMediaForUser(task.getUserId(), List.of(media.storageKey())));
                    }
                    return repository.finishRunningTask(task.getId(), STATUS_SUCCEEDED, result.toJSONString(), null)
                            .flatMap(updated -> {
                                if (Boolean.TRUE.equals(updated)) {
                                    log.info("视频合成任务执行成功: taskId={}, storageKey={}", task.getId(), media.storageKey());
                                    return Mono.<Void>empty();
                                }
                                log.info("视频合成任务已取消或已结束，清理未回填的成片媒体: taskId={}, storageKey={}", task.getId(), media.storageKey());
                                return persistenceService.deleteMediaForUser(task.getUserId(), List.of(media.storageKey()));
                            });
                });
    }

    /**
     * 根据异常完成失败或取消任务。
     *
     * @param task VideoCompositionTask 任务快照
     * @param exception Throwable 异常
     * @return Mono<Void> 更新结果
     */
    private Mono<Void> finishFailed(VideoCompositionTask task, Throwable exception) {
        boolean canceled = exception instanceof VideoCompositionMediaProcessor.VideoCompositionCanceledException;
        return cancellation.isCancellationRequested(task.getId())
                .onErrorReturn(false)
                .flatMap(cancelRequested -> {
                    if (canceled || Boolean.TRUE.equals(cancelRequested)) {
                        return repository.finishRunningTask(task.getId(), STATUS_CANCELED, "{}", SystemMessage.of("system.taskCanceled"))
                                .then(cleanupDirectorFrames(task));
                    }
                    SystemMessage message = SystemMessage.fromThrowable(exception);
                    log.error("视频合成任务执行失败: taskId={}", task.getId(), exception);
                    return repository.finishRunningTask(task.getId(), STATUS_FAILED, "{}", message)
                            .then(cleanupDirectorFrames(task));
                });
    }

    /** 校验导演台导出固定预设。 */
    private void validateDirectorDeskExportRequest(DirectorDeskExportDtos.CreateExportSessionRequest request) {
        if (request == null) throw new BusinessException(ErrorCode.PARAM_MISSING, SystemMessage.of("business.directorExportParametersAreRequired"));
        if (request.frameRate() != DIRECTOR_DESK_FRAME_RATE) throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorExportFrameRateMustBe30Fps"));
        if (request.durationMilliseconds() < DIRECTOR_DESK_MINIMUM_DURATION_MILLISECONDS || request.durationMilliseconds() > DIRECTOR_DESK_MAXIMUM_DURATION_MILLISECONDS) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorVideoDurationMustBe215Seconds"));
        }
        if (request.durationMilliseconds() % 1000 != 0 || request.frameCount() != request.durationMilliseconds() / 1000 * DIRECTOR_DESK_FRAME_RATE) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorDurationAndFrameCountMustUseWholeSecondsAt"));
        }
        boolean validSize = (request.width() == 1280 && request.height() == 720)
                || (request.width() == 720 && request.height() == 1280)
                || (request.width() == 720 && request.height() == 720);
        if (!validSize) throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorExportDimensionsDoNotMatchSupportedPresets"));
        if (request.projectSnapshot() == null || !request.projectSnapshot().containsKey("schemaVersion")) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.invalidDirectorProjectSnapshot"));
        }
    }

    /** 校验当前画布中的导演台节点与客户端工程修订一致。 */
    private Mono<Void> validateDirectorDeskProjectSnapshot(JSONObject project, DirectorDeskExportDtos.CreateExportSessionRequest request) {
        JSONObject scene = project == null ? null : project.getJSONObject("scene");
        JSONArray nodes = scene == null ? null : scene.getJSONArray("nodes");
        JSONObject node = nodes == null ? null : nodes.stream()
                .map(item -> item instanceof JSONObject value ? value : null)
                .filter(item -> item != null && request.directorNodeId().equals(item.getString("id")))
                .findFirst()
                .orElse(null);
        if (node == null || !"directorDesk".equals(node.getString("kind"))) {
            return Mono.error(new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorNodeNotFoundOnCanvas")));
        }
        JSONObject director = node.getJSONObject("director");
        if (director == null || director.getLongValue("projectRevision") != request.projectRevision()) {
            return Mono.error(new BusinessException(ErrorCode.BUSINESS_ERROR, SystemMessage.of("business.projectRevisionIsNotSavedRefreshAndRetry")));
        }
        JSONObject savedProject = director.getJSONObject("project");
        if (savedProject == null || !canonicalJson(savedProject).equals(canonicalJson(request.projectSnapshot()))) {
            return Mono.error(new BusinessException(ErrorCode.BUSINESS_ERROR, SystemMessage.of("business.exportSnapshotDiffersFromSavedProjectRefreshAndRetry")));
        }
        JSONArray cameras = savedProject.getJSONArray("cameras");
        boolean cameraExists = cameras != null && cameras.stream().anyMatch(item -> item instanceof JSONObject value && request.cameraId().equals(value.getString("id")));
        if (!cameraExists) {
            return Mono.error(new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorCameraNotFound")));
        }
        return Mono.empty();
    }

    /** 将JSON对象按键排序后序列化，消除属性写入顺序差异。 */
    private String canonicalJson(Object value) {
        return JSON.toJSONString(canonicalJsonValue(value));
    }

    /** 递归规范化JSON对象与数组。 */
    private Object canonicalJsonValue(Object value) {
        if (value instanceof JSONObject object) {
            Map<String, Object> sorted = new LinkedHashMap<>();
            object.keySet().stream().sorted().forEach(key -> sorted.put(key, canonicalJsonValue(object.get(key))));
            return sorted;
        }
        if (value instanceof JSONArray array) {
            return array.stream().map(this::canonicalJsonValue).toList();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new LinkedHashMap<>();
            map.forEach((key, item) -> sorted.put(String.valueOf(key), canonicalJsonValue(item)));
            return sorted;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(this::canonicalJsonValue).toList();
        }
        return value;
    }

    /** 读取导演台任务快照。 */
    private JSONObject parseDirectorSnapshot(VideoCompositionTask task) {
        if (!TASK_TYPE_DIRECTOR_DESK_EXPORT.equals(task.getTaskType())) throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.taskIsNotADirectorExport"));
        JSONObject snapshot = parseJsonObject(task.getSourceStorageKeys());
        if (!TASK_TYPE_DIRECTOR_DESK_EXPORT.equals(snapshot.getString("kind"))) throw new BusinessException(ErrorCode.BUSINESS_ERROR, SystemMessage.of("business.invalidDirectorExportSnapshot"));
        if (snapshot.getJSONArray("frames") == null) snapshot.put("frames", new JSONArray());
        return snapshot;
    }

    /** 校验准备任务与单帧参数一致。 */
    private void ensureDirectorPreparingTask(VideoCompositionTask task, JSONObject snapshot, int frameNumber, int width, int height, String sha256) {
        if (!"preparing".equals(task.getStatus())) throw new BusinessException(ErrorCode.BUSINESS_ERROR, SystemMessage.of("business.directorExportIsSealedCannotUploadMoreFrames"));
        if (frameNumber < 1 || frameNumber > snapshot.getIntValue("frameCount")) throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorFrameNumberIsOutOfRange"));
        if (width != snapshot.getIntValue("width") || height != snapshot.getIntValue("height")) throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorFrameDimensionsDifferFromPreset"));
        if (!StringUtils.hasText(sha256) || !sha256.matches("[0-9a-fA-F]{64}")) throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.invalidDirectorFrameDigest"));
    }

    /** 查找已接收帧，支持重复上传幂等。 */
    private JSONObject findFrame(JSONArray frames, int frameNumber) {
        if (frames == null) return null;
        return frames.stream()
                .map(item -> item instanceof JSONObject value ? value : null)
                .filter(item -> item != null && item.getIntValue("frameNumber") == frameNumber)
                .findFirst()
                .orElse(null);
    }

    /** 校验导演台帧序列完整、连续且尺寸一致。 */
    private void validateDirectorFrameManifest(JSONObject snapshot, int frameCount) {
        int expected = snapshot.getIntValue("frameCount");
        if (frameCount != expected) throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.sealedFrameCountDiffersFromPreset"));
        JSONArray frames = snapshot.getJSONArray("frames");
        if (frames == null || frames.size() != expected) throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorExportHasMissingFrames"));
        for (int index = 0; index < expected; index += 1) {
            JSONObject frame = findFrame(frames, index + 1);
            if (frame == null || frame.getIntValue("width") != snapshot.getIntValue("width") || frame.getIntValue("height") != snapshot.getIntValue("height") || !StringUtils.hasText(frame.getString("storageKey"))) {
                throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.directorFrameSequenceOrDimensionsAreInconsistent"));
            }
        }
    }

    /** 清理导演台临时帧媒体。 */
    private Mono<Void> cleanupDirectorFrames(VideoCompositionTask task) {
        if (!TASK_TYPE_DIRECTOR_DESK_EXPORT.equals(task.getTaskType())) return Mono.empty();
        return repository.getTask(task.getUserId(), task.getId())
                .defaultIfEmpty(task)
                .flatMap(this::deleteDirectorFrameMedia);
    }

    /** 根据任务最新快照删除导演台临时帧媒体。 */
    private Mono<Void> deleteDirectorFrameMedia(VideoCompositionTask task) {
        JSONObject snapshot = parseDirectorSnapshot(task);
        JSONArray frames = snapshot.getJSONArray("frames");
        List<String> storageKeys = frames == null ? List.of() : frames.stream()
                .map(item -> item instanceof JSONObject value ? value.getString("storageKey") : null)
                .filter(StringUtils::hasText)
                .toList();
        return persistenceService.deleteMediaForUser(task.getUserId(), storageKeys);
    }

    /**
     * 限制当前实例同时接收的导演台帧上传数量。
     *
     * @param operation Supplier<Mono<T>> 帧上传操作
     * @param <T> 返回值类型
     * @return Mono<T> 带并行度限制的上传结果
     */
    private <T> Mono<T> withDirectorDeskUploadSlot(Supplier<Mono<T>> operation) {
        return Mono.defer(() -> {
            int maximumUploads = Math.max(1, properties.getAi().getVideoComposition().getDirectorDeskMaximumConcurrentUploads());
            int currentUploads = directorDeskUploadCount.incrementAndGet();
            if (currentUploads > maximumUploads) {
                directorDeskUploadCount.decrementAndGet();
                return Mono.error(new BusinessException(ErrorCode.BUSINESS_ERROR, SystemMessage.of("business.directorFrameUploadConcurrencyLimitReachedRetryLater")));
            }
            return operation.get().doFinally(signalType -> directorDeskUploadCount.decrementAndGet());
        });
    }

    /**
     * 将请求中的源视频存储键归一化并校验。
     *
     * @param sourceStorageKeys List<String> 原始存储键
     * @return List<String> 归一化后的存储键
     */
    private List<String> normalizeSourceStorageKeys(List<String> sourceStorageKeys) {
        if (sourceStorageKeys == null || sourceStorageKeys.size() < 2) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.atLeastTwoVideosAreRequiredForComposition"));
        }
        if (sourceStorageKeys.size() > MAXIMUM_SOURCE_VIDEO_COUNT) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.atMost20VideosPerComposition"));
        }
        List<String> normalized = sourceStorageKeys.stream().map(value -> value == null ? "" : value.trim()).toList();
        if (normalized.stream().anyMatch(value -> !StringUtils.hasText(value))) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.videoStorageKeyIsRequired"));
        }
        Set<String> uniqueStorageKeys = new HashSet<>(normalized);
        if (uniqueStorageKeys.size() != normalized.size()) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, SystemMessage.of("business.theSameVideoCannotBeComposedTwice"));
        }
        return normalized;
    }

    /**
     * 解析任务保存的源视频存储键快照。
     *
     * @param sourceStorageKeys String JSON存储键数组
     * @return List<String> 源视频存储键
     */
    private List<String> parseSourceStorageKeys(String sourceStorageKeys) {
        try {
            return normalizeSourceStorageKeys(JSON.parseArray(sourceStorageKeys, String.class));
        } catch (BusinessException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new BusinessException(ErrorCode.BUSINESS_ERROR, SystemMessage.of("business.compositionTaskHasNoValidSourceVideoSnapshot"));
        }
    }

    /**
     * 转换任务实体为接口响应。
     *
     * @param task VideoCompositionTask 任务实体
     * @return VideoCompositionTaskResponse 接口响应
     */
    private VideoCompositionDtos.VideoCompositionTaskResponse taskResponse(VideoCompositionTask task) {
        return new VideoCompositionDtos.VideoCompositionTaskResponse(
                task.getId(), task.getStatus(), task.getProgress(), readTaskSourceStorageKeys(task),
                parseJsonObject(task.getResultData()), task.getErrorMessage(), SystemMessage.stored(task.getErrorMessageKey(), task.getErrorMessageArguments()), formatTime(task.getStartedAt()),
                formatTime(task.getCompletedAt()), formatTime(task.getCreatedAt()), formatTime(task.getUpdatedAt())
        );
    }

    /** 读取任务对外展示的源媒体键；导演台任务从帧清单展开。 */
    private List<String> readTaskSourceStorageKeys(VideoCompositionTask task) {
        if (!TASK_TYPE_DIRECTOR_DESK_EXPORT.equals(task.getTaskType())) return parseSourceStorageKeys(task.getSourceStorageKeys());
        JSONObject snapshot = parseDirectorSnapshot(task);
        JSONArray frames = snapshot.getJSONArray("frames");
        return frames == null ? List.of() : frames.stream()
                .map(item -> item instanceof JSONObject value ? value : null)
                .filter(item -> item != null)
                .sorted(Comparator.comparingInt(item -> item.getIntValue("frameNumber")))
                .map(item -> item.getString("storageKey"))
                .filter(StringUtils::hasText)
                .toList();
    }

    /**
     * 判断任务是否已结束。
     *
     * @param status String 任务状态
     * @return boolean 是否已结束
     */
    private boolean isTerminal(String status) {
        return STATUS_SUCCEEDED.equals(status) || STATUS_FAILED.equals(status) || STATUS_CANCELED.equals(status);
    }

    /**
     * 将异常转换为可展示的错误消息。
     *
     * @param exception Throwable 原始异常
     * @return String 错误消息
     */
    private String errorMessage(Throwable exception) {
        String message = exception == null ? "" : exception.getMessage();
        return StringUtils.hasText(message) ? message : "视频合成失败";
    }

    /**
     * 解析JSON对象。
     *
     * @param value String JSON文本
     * @return JSONObject 解析结果
     */
    private JSONObject parseJsonObject(String value) {
        try {
            JSONObject result = JSON.parseObject(value);
            return result == null ? new JSONObject() : result;
        } catch (Exception exception) {
            return new JSONObject();
        }
    }

    /**
     * 格式化时间。
     *
     * @param value OffsetDateTime 时间
     * @return String ISO时间文本
     */
    private String formatTime(OffsetDateTime value) {
        return value == null ? null : DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(value);
    }

    /**
     * 将时长安全转换为整型毫秒数。
     *
     * @param durationMilliseconds long 时长毫秒数
     * @return Integer 整型时长毫秒数
     */
    private Integer durationAsInteger(long durationMilliseconds) {
        return (int) Math.min(Integer.MAX_VALUE, Math.max(0L, durationMilliseconds));
    }

    /**
     * 临时目录中的成片文件及其探测结果。
     *
     * @param path Path 成片文件路径
     * @param result CompositionResult 成片媒体结果
     */
    private record CompositionOutput(Path path, VideoCompositionMediaProcessor.CompositionResult result) {
    }
}
