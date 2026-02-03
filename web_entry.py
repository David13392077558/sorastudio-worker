from flask import Flask
import threading
from processor_worker import run_worker

app = Flask(__name__)

@app.route("/")
def home():
    return "worker running"

# ⭐ Flask 加载时启动 worker 主循环
threading.Thread(target=run_worker, daemon=True).start()

# ⭐ Render 需要 Flask 监听 PORT
if __name__ != "__main__":
    # gunicorn 会走这里
    pass
else:
    # 本地调试用
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
