from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Facebook đang thức và hoạt động 24/7!"

def run():
    # Render sẽ tự động tìm kiếm các web server chạy ở port 0.0.0.0
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    # Chạy Flask server trên một luồng (thread) riêng biệt để không chặn code của bot
    t = Thread(target=run)
    t.start()