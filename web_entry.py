from flask import Flask
import threading
from processor_worker import run_worker   # 你的 worker 文件名

app = Flask(__name__)

@app.route("/")
def home():
    return "worker running"

# ⭐ 关键：Flask 启动时自动启动 worker 主循环
threading.Thread(target=run_worker, daemon=True).start()
