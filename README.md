# Отчёт по лабораторной работе №2. Hadoop & Apache Spark.

**Цель работы:**
Исследовать, как конфигурация кластера Hadoop и оптимизации Apache Spark влияют на производительность обработки больших данных.
Для этого провести 4 эксперимента:
 * Spark на кластере с 1 DataNode, без оптимизаций
 * Spark на кластере с 1 DataNode, с оптимизациями
 * Spark на кластере с 3 DataNode, без оптимизаций
 * Spark на кластере с 3 DataNode, с оптимизациями

##  Датасет
Датасет был сгенерирован в Jupyter ноутбуке `dataset_create.ipynb` и сохранен в `ecommerce_orders.csv`.

**Тема датасета**

Имитация записей заказов интернет-магазина за один год.
Каждая строка — это одна позиция в заказе.

**Столбцы**
* order_id — уникальный номер заказа, обычный ID.
* order_date — дата и время заказа. Хранится как строка в формате ГГГГ-ММ-ДД ЧЧ:ММ:СС.
* product_category — категория товара (электроника, одежда, товары для дома, книги, спорт). Это категориальный признак.
* quantity — количество товаров в заказе.
* unit_price — цена за единицу товара.
* delivery_region — регион доставки (Москва, СПб, Казань, Новосибирск, удаленные регионы). Второй категориальный признак.
* is_express — флаг экспресс-доставки (да/нет).
* customer_rating — оценка клиента от 1 до 5. В этом признаке мы намеренно оставили около 5% пропусков.
* total_amount — итоговая сумма строки заказа. Вычисляемый признак (quantity умножить на unit_price).

## Архитектура проекта
```
spark_lab/
│
├── charts/                         # Готовые графики сравнения экспериментов
│   ├── chart_comparison.png
│   ├── chart_speedup.png
│   └── chart_steps.png
│
├── data/                           # Данные
│   ├── dataset_create.ipynb        # Ноутбук генерации датасета
│   └── ecommerce_orders.csv        # Датасет (120 000 строк)
│
├── docker/                         # Конфигурации Hadoop-кластера
│   ├── docker-compose-1dn.yml      # 1 NameNode + 1 DataNode
│   ├── docker-compose-3dn.yml      # 1 NameNode + 3 DataNode
│   └── hadoop.env                  # Переменные окружения Hadoop
│
├── results/                         # Результаты измерений эксп-в в формате JSON
│   ├── 1dn_base.json
│   ├── 1dn_opt.json
│   ├── 3dn_base.json
│   └── 3dn_opt.json
│
├── scripts/                        # Скрипты запуска экспериментов
│   ├── run_1dn_base.sh
│   ├── run_1dn_opt.sh
│   ├── run_3dn_base.sh
│   └── run_3dn_opt.sh
│
├── spark/                          # Spark-приложения
│   ├── spark_app.py                # Базовый вариант
│   └── spark_app_opt.py            # Оптимизированный вариант
│
├── final_results_plots.py          # Построение итоговых графиков
├── .gitignore
└── README.md
```
## Стек
| Технология | Назначение |
|---|---|
| Apache Hadoop 3.x | Распределённое хранилище (HDFS) |
| Apache Spark 3.x | Обработка данных |
| Docker Compose | Развёртывание кластера |
| Python 3.10+ | Язык разработки |
| Jupyter Notebook | Генерация датасета |
| PyCharm | IDE |
| Git Bash | Командная строка для запуска скриптов (.sh)|

##  Запуск

**1. Настройка окружения**
Клонировать репозиторий:
```
git clone https://github.com/Arugulaa/Lab-2-Spark-Hadoop.git
cd Lab-2-Spark-Hadoop
```
Создать виртуальное окружение и активировать его:
```
python -m venv .venv
# Для Windows (Git Bash):
source .venv/Scripts/activate
# Для Linux / WSL:
source .venv/bin/activate
```
Установить зависимости:
```
pip install -r requirements.txt
```
**2. Запуск экспериментов**

2.1. Открыть *Git Bash*.

2.2. Перейти в папку проекта (в которую клонировался репозиторий). Пример:
```
cd /С/projects/spark_lab  
```
2.3. Запустить последовательно в *Git Bash* cледующие команды:
```
# 1 DataNode, базовый вариант
bash scripts/run_1dn_base.sh

# 1 DataNode, оптимизированный вариант
bash scripts/run_1dn_opt.sh

# 3 DataNode, базовый вариант
bash scripts/run_3dn_base.sh

# 3 DataNode, оптимизированный вариант
bash scripts/run_3dn_opt.sh

```

