freeze:
	pip freeze > requirements.txt

install:
	pip install -r requirements.txt

run_celery:
	celery worker -A app.celery --loglevel=info
