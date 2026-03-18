web: gunicorn --worker-class gthread --workers ${WEB_CONCURRENCY:-4} --threads ${GUNICORN_THREADS:-8} --timeout 180 --graceful-timeout 30 -b 0.0.0.0:$PORT mccain_capital.wsgi:app
