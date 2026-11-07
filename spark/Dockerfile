FROM apache/spark:3.5.3-python3
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir redis==5.0.8
USER spark
