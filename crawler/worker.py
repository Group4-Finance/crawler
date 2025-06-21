from celery import Celery

app = Celery(
    "task",
    # 只包含 tasks.py 裡面的程式, 才會成功執行
    include=["crawler.tasks", "crawler.tasks_crawler_finmind"],
    # 連線到 rabbitmq,
    # pyamqp://user:password@127.0.0.1:5672/
    # 帳號密碼都是 worker
    broker="pyamqp://worker:worker@rabbitmq:5672/",
)
