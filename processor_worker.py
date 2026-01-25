import os
import cv2
import json
import time
import redis
import ffmpeg
from pathlib import Path

# Redis连接
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

def process_video_task(task_data):
    """
    处理视频生成和处理任务
    支持：视频切片、合并、风格迁移、数字人合成等
    """
    print(f"正在处理任务: {task_data}")

    task_type = task_data.get('type')
    task_id = task_data.get('task_id')

    try:
        if task_type == 'video_generation':
            # 视频生成任务
            result = generate_video_with_sora(task_data)
        elif task_type == 'video_analysis':
            # 视频分析任务
            result = analyze_video_style(task_data)
        elif task_type == 'digital_human':
            # 数字人合成任务
            result = generate_digital_human_video(task_data)
        elif task_type == 'video_processing':
            # 视频处理任务（切片、合并等）
            result = process_video_file(task_data)
        else:
            raise ValueError(f"未知任务类型: {task_type}")

        # 更新任务状态为完成
        update_task_status(task_id, 'completed', 100, result)
        print(f"任务完成: {task_id}")

    except Exception as e:
        print(f"任务失败: {task_id}, 错误: {str(e)}")
        update_task_status(task_id, 'failed', 0, None, str(e))

def generate_video_with_sora(task_data):
    """
    使用Sora或其他模型生成视频
    这里是模拟实现，实际需要集成真实的AI模型API
    """
    prompt = task_data.get('prompt', '')
    style = task_data.get('style', 'cinematic')
    duration = task_data.get('duration', 5)

    print(f"生成视频 - 提示词: {prompt}, 风格: {style}, 时长: {duration}s")

    # 模拟视频生成过程
    time.sleep(5)  # 模拟处理时间

    # 这里应该调用真实的Sora API或ComfyUI等
    # 暂时返回模拟结果
    return {
        'video_url': f'/generated/{task_data.get("task_id")}.mp4',
        'thumbnail_url': f'/thumbnails/{task_data.get("task_id")}.jpg',
        'duration': duration,
        'resolution': '1920x1080',
        'format': 'mp4'
    }

def analyze_video_style(task_data):
    """
    分析视频风格
    """
    video_path = task_data.get('video_path')

    if not video_path or not os.path.exists(video_path):
        raise FileNotFoundError(f"视频文件不存在: {video_path}")

    print(f"分析视频风格: {video_path}")

    # 使用OpenCV分析视频
    cap = cv2.VideoCapture(video_path)
    frames = []

    # 抽取关键帧
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)

    for i in range(0, frame_count, int(fps)):  # 每秒抽一帧
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if ret:
            frames.append(frame)

    cap.release()

    # 简单的风格分析（实际应该使用AI模型）
    style_tags = []
    if len(frames) > 0:
        # 分析色彩、构图等
        avg_brightness = sum(cv2.mean(frame)[0] for frame in frames) / len(frames)
        if avg_brightness > 150:
            style_tags.append('明亮')
        elif avg_brightness < 100:
            style_tags.append('暗色调')

        # 简单的运动检测
        motion_score = 0
        for i in range(1, len(frames)):
            diff = cv2.absdiff(frames[i-1], frames[i])
            motion_score += cv2.mean(diff)[0]

        if motion_score / len(frames) > 50:
            style_tags.append('动态')
        else:
            style_tags.append('静态')

    return {
        'style_tags': style_tags,
        'frame_count': len(frames),
        'fps': fps,
        'duration': frame_count / fps if fps > 0 else 0,
        'resolution': f"{int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))}x{int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))}"
    }

def generate_digital_human_video(task_data):
    """
    生成数字人视频
    需要集成Wav2Lip、SadTalker等模型
    """
    script = task_data.get('script', '')
    avatar_image = task_data.get('avatar_image')

    print(f"生成数字人视频 - 脚本: {script[:50]}...")

    # 模拟数字人生成过程
    time.sleep(10)

    # 这里应该调用真实的数字人生成API
    return {
        'video_url': f'/digital_human/{task_data.get("task_id")}.mp4',
        'audio_url': f'/audio/{task_data.get("task_id")}.wav',
        'lip_sync_score': 0.95,
        'processing_time': 10
    }

def process_video_file(task_data):
    """
    处理视频文件：切片、合并、水印等
    """
    operation = task_data.get('operation', 'slice')
    input_path = task_data.get('input_path')
    output_path = task_data.get('output_path')

    if not input_path or not os.path.exists(input_path):
        raise FileNotFoundError(f"输入文件不存在: {input_path}")

    print(f"处理视频文件 - 操作: {operation}, 输入: {input_path}")

    # 使用ffmpeg处理视频
    if operation == 'slice':
        start_time = task_data.get('start_time', 0)
        duration = task_data.get('duration', 10)

        stream = ffmpeg.input(input_path, ss=start_time, t=duration)
        stream = ffmpeg.output(stream, output_path, vcodec='libx264', acodec='aac')
        ffmpeg.run(stream, overwrite_output=True)

    elif operation == 'merge':
        # 合并多个视频片段
        inputs = task_data.get('inputs', [])
        streams = [ffmpeg.input(path) for path in inputs]
        stream = ffmpeg.concat(*streams)
        stream = ffmpeg.output(stream, output_path, vcodec='libx264', acodec='aac')
        ffmpeg.run(stream, overwrite_output=True)

    elif operation == 'add_watermark':
        watermark_path = task_data.get('watermark_path')
        if watermark_path and os.path.exists(watermark_path):
            main = ffmpeg.input(input_path)
            watermark = ffmpeg.input(watermark_path)
            stream = ffmpeg.filter([main, watermark], 'overlay', 10, 10)
            stream = ffmpeg.output(stream, output_path)
            ffmpeg.run(stream, overwrite_output=True)

    return {
        'output_path': output_path,
        'operation': operation,
        'processed_at': time.time()
    }

def update_task_status(task_id, status, progress, result=None, error=None):
    """
    更新任务状态到Redis
    """
    status_data = {
        'task_id': task_id,
        'status': status,
        'progress': progress,
        'timestamp': time.time()
    }

    if result:
        status_data['result'] = result
    if error:
        status_data['error'] = error

    redis_client.setex(f"task:{task_id}", 3600, json.dumps(status_data))  # 1小时过期

if __name__ == "__main__":
    print("AI Worker 已启动，监听任务队列...")

    # 这里应该集成消息队列系统，如Redis Queue、RabbitMQ等
    # 暂时使用简单的轮询方式模拟

    while True:
        try:
            # 检查Redis中的待处理任务
            task_keys = redis_client.keys("pending_task:*")
            for key in task_keys:
                task_data = json.loads(redis_client.get(key))
                redis_client.delete(key)  # 移除待处理任务

                # 处理任务
                process_video_task(task_data)

        except Exception as e:
            print(f"Worker错误: {str(e)}")

        time.sleep(1)  # 每秒检查一次
