import os
import json
import time
import redis
import ffmpeg
import cv2
from pathlib import Path

redis_client = redis.from_url(
    os.environ["REDIS_URL"],
    decode_responses=True
)

print("🔌 Redis connected in Worker")

def process_video_task(task_data):
    print(f"正在处理任务: {task_data}")

    task_type = task_data.get('type')
    task_id = task_data.get('task_id')

    try:
        if task_type == 'video_generation':
            result = generate_video_with_sora(task_data)
        elif task_type == 'video_analysis':
            result = analyze_video_style(task_data)
        elif task_type == 'digital_human':
            result = generate_digital_human_video(task_data)
        elif task_type == 'video_processing':
            result = process_video_file(task_data)
        else:
            raise ValueError(f"未知任务类型: {task_type}")

        update_task_status(task_id, "completed", 100, result)
        print(f"✅ 任务完成: {task_id}")

    except Exception as e:
        print(f"❌ 任务失败: {task_id}, 错误: {str(e)}")
        update_task_status(task_id, 'failed', 0, None, str(e))


def generate_video_with_sora(task_data):
    prompt = task_data.get('prompt', '')
    style = task_data.get('style', 'cinematic')
    duration = task_data.get('duration', 5)

    print(f"生成视频 - 提示词: {prompt}, 风格: {style}, 时长: {duration}s")

    time.sleep(5)

    return {
        'video_url': f'/generated/{task_data.get("task_id")}.mp4',
        'thumbnail_url': f'/thumbnails/{task_data.get("task_id")}.jpg',
        'duration': duration,
        'resolution': '1920x1080',
        'format': 'mp4'
    }


def analyze_video_style(task_data):
    video_path = task_data.get('video_path')

    if not video_path or not os.path.exists(video_path):
        raise FileNotFoundError(f"视频文件不存在: {video_path}")

    print(f"分析视频风格: {video_path}")

    cap = cv2.VideoCapture(video_path)
    frames = []

    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)

    for i in range(0, frame_count, int(fps)):
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if ret:
            frames.append(frame)

    cap.release()

    style_tags = []
    if len(frames) > 0:
        avg_brightness = sum(cv2.mean(frame)[0] for frame in frames) / len(frames)
        if avg_brightness > 150:
            style_tags.append('明亮')
        elif avg_brightness < 100:
            style_tags.append('暗色调')

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
    script = task_data.get('script', '')
    print(f"生成数字人视频 - 脚本: {script[:50]}...")

    time.sleep(10)

    return {
        'video_url': f'/digital_human/{task_data.get("task_id")}.mp4',
        'audio_url': f'/audio/{task_data.get("task_id")}.wav',
        'lip_sync_score': 0.95,
        'processing_time': 10
    }


def process_video_file(task_data):
    operation = task_data.get('operation', 'slice')
    input_path = task_data.get('input_path')
    output_path = task_data.get('output_path')

    if not input_path or not os.path.exists(input_path):
        raise FileNotFoundError(f"输入文件不存在: {input_path}")

    print(f"处理视频文件 - 操作: {operation}, 输入: {input_path}")

    if operation == 'slice':
        start_time = task_data.get('start_time', 0)
        duration = task_data.get('duration', 10)
        stream = ffmpeg.input(input_path, ss=start_time, t=duration)
        stream = ffmpeg.output(stream, output_path, vcodec='libx264', acodec='aac')
        ffmpeg.run(stream, overwrite_output=True)

    return {
        'output_path': output_path,
        'operation': operation,
        'processed_at': time.time()
    }


def update_task_status(task_id, status, progress, result=None, error=None):
    status_data = {
        "task_id": task_id,
        "status": status,
        "progress": progress,
        "timestamp": time.time(),
    }

    if result is not None:
        status_data["result"] = result
    if error is not None:
        status_data["error"] = error

    redis_client.setex(f"task:{task_id}", 3600, json.dumps(status_data))


def run_worker():
    print("🚀 AI Worker 已启动，监听任务队列 pending_task:* ...")

    while True:
        try:
            task_keys = redis_client.keys("pending_task:*")
            for key in task_keys:
                raw = redis_client.get(key)
                if not raw:
                    redis_client.delete(key)
                    continue

                task_data = json.loads(raw)
                redis_client.delete(key)

                process_video_task(task_data)

        except Exception as e:
            print(f"⚠️ Worker 错误: {str(e)}")

        time.sleep(1)
