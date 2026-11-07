FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/ml-production-pipeline/src:/opt/ml-production-pipeline \
    HOME=/tmp

WORKDIR /opt/ml-production-pipeline

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin appuser

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY config ./config
COPY feature_store ./feature_store

USER 10001
ENTRYPOINT ["python", "-m"]
CMD ["inference.batch_score"]
