import os
import json
import time
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
HF_API_URL = os.getenv("HF_API_URL")
HF_API_KEY = os.getenv("HF_API_KEY")

if not HF_API_URL or not HF_API_KEY:
    print("⚠️ 未配置 HF_API_URL 或 HF_API_KEY，Worker 将无法调用模型 API。")


# =========================
# 通用 HuggingFace API 调用函数
# =========================
def call_hf_inference(payload: dict) -> dict:
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


# =========================
# 图像生成任务 Payload
# =========================
def build_image_generation_payload(task_data: dict) -> dict:
    prompt = task_data.get("prompt", "")

    return {
        "inputs": prompt,
        "parameters": {
            "guidance_scale": 3.5,
            "num_inference_steps": 28
        },
        "options": {
            "wait_for_model": True
        }
    }


# =========================
# 图像生成任务处理
# =========================
def process_image_generation(task_data: dict):
    print(f"🖼️ 正在生成图像: {task_data}")

    payload = build_image_generation_payload(task_data)
    result = call_hf_inference(payload)

    return result


# =========================
# 任务分发器
# =========================
def process_task(task_data: dict):
    task_type = task_data.get("type")
    task_id = task_data.get("task_id")

    if not task_id:
        print("⚠️ 任务缺少 task_id，跳过")
        return

    try:
        if task_type == "image_generation":
            result = process_image_generation(task_data)
        else:
            raise ValueError(f"未知任务类型: {task_type}")

        if result["success"]:
            update_task_status(task_id, "completed", 100, result["data"])
            print(f"✅ 任务完成: {task_id}")
        else:
            update_task_status(task_id, "failed", 0, None, result["error"])
            print(f"❌ 任务失败: {task_id}, 错误: {result['error']}")

    except Exception as e:
        print(f"❌ 任务异常: {task_id}, 错误: {str(e)}")
        update_task_status(task_id, "failed", 0, None, str(e))


# =========================
# 更新任务状态
# =========================
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


# =========================
# =========================
# Worker 主循环
# =========================
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

                process_task(task_data)

        except Exception as e:
            print(f"⚠️ Worker 错误: {str(e)}")

        time.sleep(1)


if __name__ == "__main__":
    run_worker()
