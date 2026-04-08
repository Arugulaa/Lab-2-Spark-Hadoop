import os
import time
import json
import psutil
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ──────────────────────────────────────────
# Конфигурация эксперимента
# ──────────────────────────────────────────

# os.environ.get — читаем значение из docker-compose, не хардкодим
EXPERIMENT_ID = os.environ.get("EXPERIMENT_ID", "1dn_opt")

RESULTS_DIR = "/results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# ──────────────────────────────────────────
# Логгер
# ──────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="[SPARK-APP-OPT] %(asctime)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("SparkAppOpt")

# ──────────────────────────────────────────
# Вспомогательные функции
# ──────────────────────────────────────────

def get_ram_mb():
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

def run_step(spark, description, func):
    spark.sparkContext.setJobDescription(description)
    t_start = time.time()
    result = func()
    elapsed = time.time() - t_start
    logger.info(f"{description} — {elapsed:.2f} сек")
    spark.sparkContext.setJobDescription(None)
    return result, elapsed

# ──────────────────────────────────────────
# Создание Spark Session
# ──────────────────────────────────────────

logger.info(f"Эксперимент: {EXPERIMENT_ID}")
logger.info("Создаём Spark Session (оптимизированная)...")

spark = (
    SparkSession.builder
    .appName(f"EcommerceAnalysis_{EXPERIMENT_ID}")
    .master("local[*]")
    .config("spark.driver.host", "spark")           # hostname нашего контейнера
    .config("spark.driver.bindAddress", "0.0.0.0")  # слушаем все интерфейсы
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.driver.extraJavaOptions",
            "-Dlog4j2.logger.org.apache.spark.level=ERROR "
            "-Dlog4j2.logger.py4j.level=OFF")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
logger.info("Spark Session создана успешно")

# ──────────────────────────────────────────
# Замеры
# ──────────────────────────────────────────

ram_start = get_ram_mb()
time_total_start = time.time()
logger.info(f"RAM до начала работы: {ram_start:.1f} MB")

step_times = {}

# ── Шаг 1: Чтение данных из HDFS ──────────

def step_read():
    df = spark.read.csv(
        "hdfs://namenode:9000/data/input/ecommerce_orders.csv",  # исправлено
        header=True,
        inferSchema=True
    )
    count = df.count()
    logger.info(f"Строк в датасете: {count:,}")
    return df

df, step_times["чтение"] = run_step(spark, "Чтение CSV из HDFS", step_read)

# ── Шаг 1.5: Кэширование ← ОПТИМИЗАЦИЯ ───
# .cache() сохраняет df в памяти после первого action (.count() выше).
# Все следующие шаги читают данные из RAM, а не снова с HDFS.
df = df.cache()
logger.info("DataFrame закэширован в памяти")

# ── Шаг 2: Обработка пропусков ────────────

def step_fillna():
    mean_rating = df.select(F.mean("customer_rating")).first()[0]
    return df.fillna({"customer_rating": round(mean_rating, 1)})

df, step_times["пропуски"] = run_step(spark, "Обработка пропусков", step_fillna)

# ── Шаг 2.5: Repartition ← ОПТИМИЗАЦИЯ ───
# После fillna переразбиваем данные равномерно по 8 партициям.
# Это убирает перекос который мог возникнуть при чтении из HDFS-блоков.
df = df.repartition(8)
logger.info("DataFrame переразбит на 8 партиций")

# ── Шаг 3: groupBy + агрегация ────────────

def step_groupby():
    result = (
        df.groupBy("product_category")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("revenue"),
            F.avg("customer_rating").alias("avg_rating"),
            F.avg("quantity").alias("avg_quantity")
        )
        .orderBy("revenue", ascending=False)
    )
    result.show(truncate=False)
    return result

result_category, step_times["группировка"] = run_step(
    spark, "Группировка по категориям", step_groupby
)

# ── Шаг 4: Фильтрация экспресс-заказов ────

def step_express():
    result = (
        df.filter(F.col("is_express") == True)
        .filter(F.col("total_amount") > 100)
        .groupBy("delivery_region")
        .agg(
            F.count("order_id").alias("express_orders"),
            F.sum("total_amount").alias("express_revenue")
        )
        .orderBy("express_revenue", ascending=False)
    )
    result.show(truncate=False)
    return result

result_express, step_times["фильтрация"] = run_step(
    spark, "Фильтрация экспресс-заказов", step_express
)

# ── Освобождаем кэш ← ОПТИМИЗАЦИЯ ─────────
# .unpersist() убирает df из памяти — важно делать после всех шагов
df.unpersist()
logger.info("Кэш очищен")

# ── Итоговые метрики ──────────────────────

total_time = time.time() - time_total_start
ram_end = get_ram_mb()

logger.info("=" * 50)
logger.info("РЕЗУЛЬТАТЫ ЭКСПЕРИМЕНТА (OPT)")
logger.info("-" * 50)
for step_name, elapsed in step_times.items():
    logger.info(f"  {step_name:<20}: {elapsed:.2f} сек")
logger.info("-" * 50)
logger.info(f"  {'ИТОГО':<20}: {total_time:.2f} сек")
logger.info(f"  RAM начало           : {ram_start:.1f} MB")
logger.info(f"  RAM конец            : {ram_end:.1f} MB")
logger.info(f"  Прирост RAM          : {ram_end - ram_start:.1f} MB")
logger.info("=" * 50)

# ── Сохранение результатов в JSON ─────────

result_data = {
    "experiment_id": EXPERIMENT_ID,
    "total_time_sec": round(total_time, 2),
    "ram_start_mb": round(ram_start, 1),
    "ram_end_mb": round(ram_end, 1),
    "ram_delta_mb": round(ram_end - ram_start, 1),
    "steps": {k: round(v, 2) for k, v in step_times.items()},
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
}

output_path = os.path.join(RESULTS_DIR, f"{EXPERIMENT_ID}.json")
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(result_data, f, indent=2, ensure_ascii=False)

logger.info(f"Результаты сохранены в {output_path}")

spark.stop()
logger.info("Spark Session остановлена")