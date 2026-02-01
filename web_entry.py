from flask import Flask
import threading
import os
import processor_worker  # 你的 worker 文件名

app = Flask(__name__)

@app.route("/")
def home():
    return "worker running"

def start_worker():
    # 调用你 worker 里的主循环
    processor_worker.run_worker()

if __name__ == "__main__":
    # 启动 worker 线程
    threading.Thread(target=start_worker, daemon=True).start()

    # Render 要求 Web Service 必须监听 PORT
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

