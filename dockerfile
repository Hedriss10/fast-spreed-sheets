FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev gcc make build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/base.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY . /app

WORKDIR /app

EXPOSE 5001

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5001", "src:create_app()"]