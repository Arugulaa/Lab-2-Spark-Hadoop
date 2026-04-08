#!/bin/bash
set -e  # останавливаем скрипт при любой ошибке
export MSYS_NO_PATHCONV=1
COMPOSE_FILE="docker/docker-compose-1dn.yml"
EXPERIMENT_ID="1dn_opt"
SPARK_SCRIPT="/app/spark_app_opt.py"

echo "[INFO] Запуск эксперимента: $EXPERIMENT_ID"

# Шаг 1: поднимаем кластер
docker compose -f $COMPOSE_FILE up -d

# Шаг 2: ждём пока NameNode готов
echo "[INFO] Ждём готовности NameNode..."
until docker exec namenode hdfs dfsadmin -report > /dev/null 2>&1; do
    echo "[INFO] NameNode ещё не готов, ждём 5 сек..."
    sleep 5
done
echo "[INFO] NameNode готов"

# Шаг 3: загружаем датасет в HDFS
docker cp data/ecommerce_orders.csv namenode:/tmp/ecommerce_orders.csv
docker exec namenode hdfs dfs -mkdir -p /data/input
docker exec namenode hdfs dfs -put -f \
    /tmp/ecommerce_orders.csv \
    /data/input/ecommerce_orders.csv
echo "[INFO] Датасет загружен в HDFS"

# Шаг 3.5: ставим psutil в контейнер
docker exec -u root spark_app python3 -m pip install -q psutil

# Шаг 4: запускаем Spark приложение
docker exec -e EXPERIMENT_ID=$EXPERIMENT_ID spark_app \
    /opt/spark/bin/spark-submit $SPARK_SCRIPT

echo "[INFO] Эксперимент $EXPERIMENT_ID завершён"

# Шаг 5: останавливаем кластер и чистим volumes
docker compose -f $COMPOSE_FILE down -v
echo "[INFO] Кластер остановлен, volumes очищены"