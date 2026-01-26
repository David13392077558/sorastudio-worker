import os
import json
import time
from pathlib import Path

import redis
import requests

# =========================
# Redis 连接
# =========================
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_password = os.getenv("REDIS_PASSWORD") or None

redis_client = redis.Redis(
    host=redis_host,
    port=redis_port,
    password=redis_password,
    decode_responses=True,
)

# =========================
# HuggingFace Inference API 配置
# =========================
HF_API_URL = os.getenv("HF_API_URL")  # 例如: https://api-inference.huggingface.co/models/xxx/xxx
HF_API_KEY = os.getenv("HF_API_KEY")  # 你的 HuggingFace Token

if not HF_API_URL or not HF_API_KEY:
    print("⚠️ 未配置 HF_API_URL 或 HF_API_KEY，Worker 启动后将无法调用模型 API。")


def call_hf_inference(payload: dict) -> dict:
    """
    调用 HuggingFace Inference API 的通用函数
    payload: 传给模型的参数（prompt、参数等）
    """
    headers = {
        "Authorization": f"Bearer {HF_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(HF_API_URL, headers=headers, json=payload, timeout=600)
        resp.raise_for_status()
        return {
            "success": True,
            "data": resp.json(),
            "status_code": resp.status_code,
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "error": str(e),
            "status_code": getattr(e.response, "status_code", None),
        }


def process_video_task(task_data: dict):
    """
    处理“视频相关任务”的统一入口
    现在不做本地视频处理，只负责：
    - 解析任务
    - 调用 HuggingFace Inference API
    - 回写结果
    """
    print(f"🎯 正在处理任务: {task_data}")

    task_type = task_data.get("type")
    task_id = task_data.get("task_id")

    if not task_id:
        print("⚠️ 任务缺少 task_id，跳过")
        return

    try:
        # 根据任务类型构造不同的 payload
        if task_type == "video_generation":
            payload = build_video_generation_payload(task_data)
        elif task_type == "video_analysis":
            payload = build_video_analysis_payload(task_data)
        elif task_type == "digital_human":
            payload = build_digital_human_payload(task_data)
        elif task_type == "video_processing":
            payload = build_video_processing_payload(task_data)
        else:
            raise ValueError(f"未知任务类型: {task_type}")

        # 调用 HuggingFace Inference API
        hf_result = call_hf_inference(payload)

        if hf_result["success"]:
            update_task_status(
                task_id,
                status="completed",
                progress=100,
                result=hf_result["data"],
            )
            print(f"✅ 任务完成: {task_id}")
        else:
            update_task_status(
                task_id,
                status="failed",
                progress=0,
                error=hf_result["error"],
            )
            print(f"❌ 任务失败: {task_id}, 错误: {hf_result['error']}")

    except Exception as e:
        print(f"❌ 任务异常: {task_id}, 错误: {str(e)}")
        update_task_status(task_id, "failed", 0, None, str(e))


# =========================
# 各类任务的 payload 构造函数
# =========================

def build_video_generation_payload(task_data: dict) -> dict:
    """
    构造视频生成任务的 payload
    这里你可以根据你实际使用的模型 API 格式来调整
    """
    prompt = task_data.get("prompt", "")
    style = task_data.get("style", "cinematic")
    duration = task_data.get("duration", 5)

    return {
        "inputs": prompt,
        "parameters": {
            "style": style,
            "duration": duration,
            # 这里可以根据模型文档添加更多参数
        },
        "options": {
            "wait_for_model": True
        }
    }


def build_video_analysis_payload(task_data: dict) -> dict:
    """
    构造视频分析任务的 payload
    注意：这里不再读取本地文件路径，而是使用远程 URL 或上游传入的标识
    """
    video_url = task_data.get("video_url")
    if not video_url:
        raise ValueError("video_analysis 任务缺少 video_url")

    return {
        "inputs": video_url,
        "parameters": {
            "task": "video_analysis"
        }
    }


def build_digital_human_payload(task_data: dict) -> dict:
    """
    构造数字人任务的 payload
    """
    script = task_data.get("script", "")
    avatar_ref = task_data.get("avatar_ref")  # 可以是 URL 或 ID

    return {
        "inputs": {
            "script": script,
            "avatar": avatar_ref,
        },
        "parameters": {
            "task": "digital_human"
        }
    }


def build_video_processing_payload(task_data: dict) -> dict:
    """
    构造视频处理任务的 payload（切片、合并等）
    这里不做本地 ffmpeg，而是交给后端模型 / 服务处理
    """
    operation = task_data.get("operation", "slice")
    source = task_data.get("source")  # 可以是 URL 或 ID

    return {
        "inputs": {
            "operation": operation,
            "source": source,
            "params": task_data.get("params", {}),
        },
        "parameters": {
            "task": "video_processing"
        }
    }


# =========================
# 任务状态更新
# =========================

def update_task_status(task_id, status, progress, result=None, error=None):
    """
    更新任务状态到 Redis
    key: task:{task_id}
    """
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

    redis_client.setex(f"task:{task_id}", 3600, json.dumps(status_data))  # 1 小时过期


# =========================
# 主循环：轮询 Redis 任务队列
# =========================

if __name__ == "__main__":
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
                redis_client.delete(key)  # 取出后删除 pending_task

                process_video_task(task_data)

        except Exception as e:
            print(f"⚠️ Worker 错误: {str(e)}")

        time.sleep(1)  # 每秒检查一次

