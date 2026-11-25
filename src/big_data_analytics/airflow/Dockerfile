FROM apache/airflow:2.5.0

USER root
RUN rm -f /etc/apt/sources.list.d/pgdg.list \
  && sed -i 's|deb.debian.org/debian-security|archive.debian.org/debian-security|g; s|deb.debian.org/debian|archive.debian.org/debian|g' /etc/apt/sources.list \
  && sed -i '/bullseye-updates/d' /etc/apt/sources.list \
  && echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid \
  && apt-get update \
  && apt-get install -y --no-install-recommends openjdk-11-jre-headless \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark confluent-kafka pandas