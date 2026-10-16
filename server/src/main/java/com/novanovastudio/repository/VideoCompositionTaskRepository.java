package com.novanovastudio.repository;

import com.alibaba.fastjson2.JSON;

import com.novanovastudio.common.SystemMessage;

import com.novanovastudio.entity.VideoCompositionTask;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 画布视频合成任务数据访问仓储。
 *
 * @author   zhenglin.cn.cq@gmail.com
 * @date     2026-08-11 00:00
 */
@Repository
@RequiredArgsConstructor
public class VideoCompositionTaskRepository {

    /** 数据库客户端 */
    private final DatabaseClient databaseClient;

    /**
     * 创建视频合成任务。
     *
     * @param task VideoCompositionTask 待保存任务
     * @return Mono<Void> 保存结果
     */
    public Mono<Void> createTask(VideoCompositionTask task) {
        return databaseClient.sql("""
                INSERT INTO video_composition_tasks(id, user_id, task_type, status, progress, source_storage_keys, result_data, error_message)
                VALUES (:id, :userId, :taskType, :status, :progress, CAST(:sourceStorageKeys AS jsonb), CAST(:resultData AS jsonb), :errorMessage)
                """)
                .bind("id", task.getId())
                .bind("userId", task.getUserId())
                .bind("taskType", task.getTaskType())
                .bind("status", task.getStatus())
                .bind("progress", task.getProgress())
                .bind("sourceStorageKeys", task.getSourceStorageKeys())
                .bind("resultData", task.getResultData())
                .bind("errorMessage", task.getErrorMessage() == null ? "" : task.getErrorMessage())
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * 按用户和任务ID查询任务。
     *
     * @param userId Long 用户ID
     * @param taskId String 任务ID
     * @return Mono<VideoCompositionTask> 任务数据
     */
    public Mono<VideoCompositionTask> getTask(Long userId, String taskId) {
        return queryTask("WHERE user_id = :userId AND id = :taskId")
                .bind("userId", userId)
                .bind("taskId", taskId)
                .map((row, metadata) -> RowMappers.videoCompositionTask(row))
                .one();
    }

    /**
     * 按任务ID查询任务。
     *
     * @param taskId String 任务ID
     * @return Mono<VideoCompositionTask> 任务数据
     */
    public Mono<VideoCompositionTask> getTaskById(String taskId) {
        return queryTask("WHERE id = :taskId")
                .bind("taskId", taskId)
                .map((row, metadata) -> RowMappers.videoCompositionTask(row))
                .one();
    }

    /**
     * 将等待中的任务原子标记为执行中。
     *
     * @param taskId String 任务ID
     * @return Mono<Boolean> 是否成功领取任务
     */
    public Mono<Boolean> markTaskRunning(String taskId) {
        return databaseClient.sql("""
                UPDATE video_composition_tasks
                SET status = 'running', progress = 0, error_message = '', error_message_key = '', error_message_arguments = '[]'::jsonb, started_at = CURRENT_TIMESTAMP, completed_at = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE id = :taskId AND status = 'pending'
                """)
                .bind("taskId", taskId)
                .fetch()
                .rowsUpdated()
                .map(count -> count != null && count > 0);
    }

    /**
     * 更新运行中任务进度。
     *
     * @param taskId String 任务ID
     * @param progress int 任务进度
     * @return Mono<Void> 更新结果
     */
    public Mono<Void> updateProgress(String taskId, int progress) {
        return databaseClient.sql("""
                UPDATE video_composition_tasks
                SET progress = GREATEST(progress, :progress), updated_at = CURRENT_TIMESTAMP
                WHERE id = :taskId AND status = 'running'
                """)
                .bind("taskId", taskId)
                .bind("progress", Math.max(0, Math.min(99, progress)))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * 完成运行中任务。
     *
     * @param taskId String 任务ID
     * @param status String 终态
     * @param resultData String 合成结果JSON
     * @param errorMessage String 错误信息
     * @return Mono<Boolean> 是否成功更新任务
     */
    public Mono<Boolean> finishRunningTask(String taskId, String status, String resultData, SystemMessage errorMessage) {
        return databaseClient.sql("""
                UPDATE video_composition_tasks
                SET status = :status, progress = 100, result_data = CAST(:resultData AS jsonb), error_message = :errorMessage, error_message_key = :errorMessageKey, error_message_arguments = CAST(:errorMessageArguments AS jsonb),
                    completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id = :taskId AND status = 'running'
                """)
                .bind("taskId", taskId)
                .bind("status", status)
                .bind("resultData", resultData == null ? "{}" : resultData)
                .bind("errorMessage", errorMessage == null ? "" : errorMessage.diagnostic())
                .bind("errorMessageKey", errorMessage == null ? "" : errorMessage.key())
                .bind("errorMessageArguments", JSON.toJSONString(errorMessage == null ? new Object[0] : errorMessage.arguments()))
                .fetch()
                .rowsUpdated()
                .map(count -> count != null && count > 0);
    }

    /**
     * 取消尚未完成的任务。
     *
     * @param userId Long 用户ID
     * @param taskId String 任务ID
     * @return Mono<Boolean> 是否成功取消任务
     */
    public Mono<Boolean> cancelTask(Long userId, String taskId) {
        return databaseClient.sql("""
                UPDATE video_composition_tasks
                SET status = 'canceled', progress = 100, error_message = '任务已取消', error_message_key = 'system.taskCanceled', error_message_arguments = '[]'::jsonb, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = :userId AND id = :taskId AND status IN ('preparing', 'pending', 'running')
                """)
                .bind("userId", userId)
                .bind("taskId", taskId)
                .fetch()
                .rowsUpdated()
                .map(count -> count != null && count > 0);
    }

    /**
     * 原子追加导演台帧清单，避免并发上传时使用旧快照覆盖其他帧。
     *
     * @param userId Long 用户ID
     * @param taskId String 任务ID
     * @param frameNumber int 帧序号
     * @param frameJson String 帧元数据JSON
     * @param frameBytes long 当前帧字节数
     * @param maximumTotalBytes long 任务允许的帧总字节数
     * @return Mono<String> 追加后的任务快照；帧已存在、任务状态不符或超出总容量时返回空
     */
    public Mono<String> appendPreparingDirectorDeskFrame(Long userId, String taskId, int frameNumber,
                                                          String frameJson, long frameBytes, long maximumTotalBytes) {
        return databaseClient.sql("""
                UPDATE video_composition_tasks
                SET source_storage_keys = jsonb_set(
                    source_storage_keys,
                    '{frames}',
                    COALESCE(source_storage_keys->'frames', '[]'::jsonb) || jsonb_build_array(CAST(:frameJson AS jsonb)),
                    true
                ), updated_at = CURRENT_TIMESTAMP
                WHERE user_id = :userId AND id = :taskId AND task_type = 'directorDeskExport' AND status = 'preparing'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements(COALESCE(source_storage_keys->'frames', '[]'::jsonb)) AS frame
                      WHERE frame->>'frameNumber' = CAST(:frameNumber AS text)
                  )
                  AND COALESCE((
                      SELECT SUM(NULLIF(frame->>'bytes', '')::bigint)
                      FROM jsonb_array_elements(COALESCE(source_storage_keys->'frames', '[]'::jsonb)) AS frame
                  ), 0) + CAST(:frameBytes AS bigint) <= :maximumTotalBytes
                RETURNING source_storage_keys::text AS source_storage_keys
                """)
                .bind("userId", userId)
                .bind("taskId", taskId)
                .bind("frameNumber", frameNumber)
                .bind("frameJson", frameJson)
                .bind("frameBytes", Math.max(0L, frameBytes))
                .bind("maximumTotalBytes", Math.max(1L, maximumTotalBytes))
                .map((row, metadata) -> row.get("source_storage_keys", String.class))
                .one();
    }

    /** 原子封存导演台帧清单并转入现有等待队列。 */
    public Mono<Boolean> sealDirectorDeskTask(Long userId, String taskId, String sourceSnapshot) {
        return databaseClient.sql("""
                UPDATE video_composition_tasks
                SET status = 'pending', progress = 0, source_storage_keys = CAST(:sourceSnapshot AS jsonb), updated_at = CURRENT_TIMESTAMP
                WHERE user_id = :userId AND id = :taskId AND task_type = 'directorDeskExport' AND status = 'preparing'
                """)
                .bind("userId", userId)
                .bind("taskId", taskId)
                .bind("sourceSnapshot", sourceSnapshot)
                .fetch()
                .rowsUpdated()
                .map(count -> count != null && count > 0);
    }

    /**
     * 查询可恢复的等待任务。
     *
     * @return Flux<VideoCompositionTask> 等待任务列表
     */
    public Flux<VideoCompositionTask> listPendingTasks() {
        return queryTask("WHERE status = 'pending' ORDER BY created_at ASC, id ASC")
                .map((row, metadata) -> RowMappers.videoCompositionTask(row))
                .all();
    }

    /**
     * 查询运行中的任务，用于根据Redis活动租约恢复已失去执行实例的任务。
     *
     * @return Flux<VideoCompositionTask> 运行中任务列表
     */
    public Flux<VideoCompositionTask> listRunningTasks() {
        return queryTask("WHERE status = 'running' ORDER BY created_at ASC, id ASC")
                .map((row, metadata) -> RowMappers.videoCompositionTask(row))
                .all();
    }

    /**
     * 查询超时未封存的导演台准备任务。
     *
     * @param cutoffTime 超时判断时间点
     * @return Flux<VideoCompositionTask> 已超时准备任务
     */
    public Flux<VideoCompositionTask> listExpiredDirectorDeskPreparingTasks(OffsetDateTime cutoffTime) {
        return queryTask("WHERE task_type = 'directorDeskExport' AND status = 'preparing' AND updated_at < :cutoffTime ORDER BY updated_at ASC, id ASC")
                .bind("cutoffTime", cutoffTime)
                .map((row, metadata) -> RowMappers.videoCompositionTask(row))
                .all();
    }

    /** 统计用户当前未封存的导演台准备任务数量。 */
    public Mono<Integer> countDirectorDeskPreparingTasks(Long userId) {
        return databaseClient.sql("""
                SELECT COUNT(*) AS task_count
                FROM video_composition_tasks
                WHERE user_id = :userId AND task_type = 'directorDeskExport' AND status = 'preparing'
                """)
                .bind("userId", userId)
                .map((row, metadata) -> row.get("task_count", Long.class))
                .one()
                .map(value -> Math.toIntExact(value))
                .defaultIfEmpty(0);
    }

    /**
     * 将没有活动执行租约的运行中任务原子重新排队。
     *
     * @param taskId String 任务ID
     * @return Mono<Boolean> 是否成功重新排队
     */
    public Mono<Boolean> requeueInactiveRunningTask(String taskId) {
        return databaseClient.sql("""
                UPDATE video_composition_tasks
                SET status = 'pending', progress = 0, error_message = '执行实例已断开，任务已重新排队', error_message_key = 'system.taskRequeued', error_message_arguments = '[]'::jsonb,
                    started_at = NULL, completed_at = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE id = :taskId AND status = 'running'
                """)
                .bind("taskId", taskId)
                .fetch()
                .rowsUpdated()
                .map(count -> count != null && count > 0);
    }

    /**
     * 组装任务查询SQL。
     *
     * @param condition String 查询条件
     * @return DatabaseClient.GenericExecuteSpec 查询规格
     */
    private DatabaseClient.GenericExecuteSpec queryTask(String condition) {
        return databaseClient.sql("""
                SELECT id, user_id, task_type, status, progress, source_storage_keys::text AS source_storage_keys, result_data::text AS result_data,
                       error_message, error_message_key, error_message_arguments::text AS error_message_arguments, started_at, completed_at, created_at, updated_at
                FROM video_composition_tasks
                """ + condition);
    }
}
