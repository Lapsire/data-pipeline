FROM python:3.11-slim

RUN pip install aiohttp redis

COPY http_job.py /scripts/http_job.py

CMD ["python", "/scripts/http_job.py"]