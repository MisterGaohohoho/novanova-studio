import { getAiTaskPollingIntervalMilliseconds, serverFetch, serverPost } from "./server";
import type { VideoCompositionResultData, VideoCompositionTask } from "./video-composition";

/** 导演台导出会话创建参数。 */
export type CreateDirectorDeskExportSessionInput = {
    canvasDocumentId: string;
    directorNodeId: string;
    projectRevision: number;
    cameraId: string;
    durationMilliseconds: number;
    width: number;
    height: number;
    frameRate: 30;
    frameCount: number;
    projectSnapshot: Record<string, unknown>;
};

/** 导演台导出任务，复用视频合成任务状态。 */
export type DirectorDeskExportTask = VideoCompositionTask & {
    resultData: VideoCompositionResultData;
};

/** 创建导演台导出准备会话。 */
export function createDirectorDeskExportSession(input: CreateDirectorDeskExportSessionInput): Promise<DirectorDeskExportTask> {
    return serverPost<DirectorDeskExportTask>("/directorDesk/createExportSession", input);
}

/** 上传单个确定序号的 PNG 帧。 */
export function uploadDirectorDeskExportFrame(input: { taskId: string; frameNumber: number; width: number; height: number; sha256: string; blob: Blob }): Promise<DirectorDeskExportTask> {
    const formData = new FormData();
    formData.set("file", input.blob, `frame-${String(input.frameNumber).padStart(6, "0")}.png`);
    formData.set("taskId", input.taskId);
    formData.set("frameNumber", String(input.frameNumber));
    formData.set("width", String(input.width));
    formData.set("height", String(input.height));
    formData.set("sha256", input.sha256);
    return serverFetch<DirectorDeskExportTask>("/directorDesk/uploadExportFrameBatch", { method: "POST", body: formData });
}

/** 封存完整帧清单并触发服务端编码。 */
export function finishDirectorDeskExportFrames(taskId: string, frameCount: number): Promise<DirectorDeskExportTask> {
    return serverPost<DirectorDeskExportTask>("/directorDesk/finishExportFrames", { taskId, frameCount });
}

/** 查询导演台导出任务。 */
export function getDirectorDeskExportTask(taskId: string): Promise<DirectorDeskExportTask> {
    return serverPost<DirectorDeskExportTask>("/ai/video/getCompositionTask", { taskId });
}

/** 轮询导演台导出任务直到编码结束。 */
export async function waitDirectorDeskExportTask(taskId: string, options: { signal?: AbortSignal; onProgress?: (task: DirectorDeskExportTask) => void } = {}): Promise<DirectorDeskExportTask> {
    const interval = await getAiTaskPollingIntervalMilliseconds();
    while (true) {
        if (options.signal?.aborted) throw new DOMException("Aborted", "AbortError");
        const task = await getDirectorDeskExportTask(taskId);
        if (["succeeded", "failed", "canceled"].includes(task.status)) return task;
        options.onProgress?.(task);
        await new Promise<void>((resolve, reject) => {
            const timer = window.setTimeout(resolve, interval);
            options.signal?.addEventListener(
                "abort",
                () => {
                    window.clearTimeout(timer);
                    reject(new DOMException("Aborted", "AbortError"));
                },
                { once: true },
            );
        });
    }
}
