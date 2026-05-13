web: gunicorn --worker-class gthread --workers ${WEB_CONCURRENCY:-2} --threads ${GUNICORN_THREADS:-2} --timeout 180 --graceful-timeout 30 -b 0.0.0.0:$PORT mccain_capital.wsgi:app
