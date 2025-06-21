# crawler


## worker

    pipenv run celery -A crawler.worker worker --loglevel=info


## producer

    pipenv run python crawler/producer.py
