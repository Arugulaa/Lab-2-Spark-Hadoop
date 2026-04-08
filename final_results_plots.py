import json
import glob
import os
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

matplotlib.use("Agg")
matplotlib.rcParams["font.family"] = "DejaVu Sans"

RESULTS_DIR = "results"
CHARTS_DIR = "charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

# Загружаем все JSON из папки results/
files = sorted(glob.glob(os.path.join(RESULTS_DIR, "*.json")))
print(f"Найдено файлов: {len(files)}")

experiments = []
for filepath in files:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    experiments.append(data)
    print(f"  Загружен: {data['experiment_id']}")

# Сортируем в нужном порядке
order = ["1dn_base", "1dn_opt", "3dn_base", "3dn_opt"]
experiments.sort(key=lambda x: order.index(x["experiment_id"]) if x["experiment_id"] in order else 99)

# Метаданные: подписи, цвета, параметры Spark для каждого эксперимента
meta = {
    "1dn_base": {"label": "1DN Базовый",      "color": "#4878cf", "shuffle": 4, "repartition": "-"},
    "1dn_opt":  {"label": "1DN Оптимизир.",   "color": "#6acc65", "shuffle": 8, "repartition": 8},
    "3dn_base": {"label": "3DN Базовый",      "color": "#d65f5f", "shuffle": 4, "repartition": "-"},
    "3dn_opt":  {"label": "3DN Оптимизир.",   "color": "#b47cc7", "shuffle": 8, "repartition": 8},
}

labels      = [meta[e["experiment_id"]]["label"] for e in experiments]
colors      = [meta[e["experiment_id"]]["color"] for e in experiments]
total_times = [e["total_time_sec"]               for e in experiments]
ram_end     = [e["ram_end_mb"]                   for e in experiments]
step_names  = list(experiments[0]["steps"].keys())
steps_data  = {s: [e["steps"].get(s, 0) for e in experiments] for s in step_names}



# График 1: Разбивка времени по шагам (stacked bars)
step_colors = ["#4878cf", "#6acc65", "#d65f5f", "#e6ac27"]
fig, ax = plt.subplots(figsize=(12, 7))
bottoms = [0.0] * len(labels)
for i, step in enumerate(step_names):
    vals = steps_data[step]
    bars = ax.bar(labels, vals, bottom=bottoms,
                  color=step_colors[i % len(step_colors)],
                  label=step, edgecolor="white")
    for bar, val, bot in zip(bars, vals, bottoms):
        if val > 0.3:
            ax.text(bar.get_x() + bar.get_width() / 2, bot + val / 2,
                    f"{val:.2f}s", ha="center", va="center",
                    fontsize=8, color="white", fontweight="bold")
    bottoms = [b + v for b, v in zip(bottoms, vals)]
for i, exp in enumerate(experiments):
    m = meta[exp["experiment_id"]]
    ax.text(i, bottoms[i] + 0.2,
            f"shuffle={m['shuffle']}\nrepartition={m['repartition']}",
            ha="center", va="bottom", fontsize=8, color="#333333")
ax.set_ylabel("Секунды", fontsize=12)
ax.set_title("Разбивка времени по шагам", fontsize=14, fontweight="bold")
ax.legend(loc="upper right")
ax.yaxis.grid(True, linestyle="--", alpha=0.5)
ax.set_ylim(0, max(bottoms) * 1.25)
ax.yaxis.set_major_locator(MultipleLocator(1))  # метка каждые 1 секунду
ax.set_axisbelow(True)
plt.tight_layout()
plt.savefig(os.path.join(CHARTS_DIR, "chart_steps.png"), dpi=150)
plt.close()
print("Сохранён: chart_steps.png")

# График 2: Время и RAM рядом
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 6))

bars1 = ax1.bar(labels, total_times, color=colors, width=0.5, edgecolor="white")
for bar, val in zip(bars1, total_times):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.2,
             f"{val:.2f}s", ha="center", fontweight="bold", fontsize=10)
# параметры внутри баров — добавляем так же как в chart_total_time
for bar, exp in zip(bars1, experiments):
    m = meta[exp["experiment_id"]]
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() / 2,
             f"shuffle={m['shuffle']}\nrepartition={m['repartition']}",
             ha="center", va="center", fontsize=8, color="white", fontweight="bold")
ax1.set_ylabel("Секунды", fontsize=11)
ax1.set_title("Время выполнения", fontsize=13, fontweight="bold")
ax1.set_ylim(0, max(total_times) * 1.2)
ax1.yaxis.set_major_locator(MultipleLocator(1))
ax1.set_xticklabels(labels, rotation=20, ha="right")
ax1.yaxis.grid(True, linestyle="--", alpha=0.5)
ax1.set_axisbelow(True)

bars2 = ax2.bar(labels, ram_end, color=colors, width=0.5, edgecolor="white")
for bar, val in zip(bars2, ram_end):
    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.2,
             f"{val:.1f}MB", ha="center", fontweight="bold", fontsize=10)
ax2.set_ylabel("RAM (МБ)", fontsize=11)
ax2.set_title("Потребление оперативной памяти", fontsize=13, fontweight="bold")
ax2.set_ylim(0, max(ram_end) * 1.2)
ax2.set_xticklabels(labels, rotation=20, ha="right")
ax2.yaxis.grid(True, linestyle="--", alpha=0.5)
ax2.set_axisbelow(True)

fig.suptitle("Влияние оптимизаций Spark: время и память", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig(os.path.join(CHARTS_DIR, "chart_comparison.png"), dpi=150)
plt.close()
print("Сохранён: chart_comparison.png")

# График 3: Ускорение (Speedup)
# Speedup = время_базового / время_сравниваемого
# Значение > 1.0 означает ускорение, < 1.0 — замедление
base_1dn = next(e["total_time_sec"] for e in experiments if e["experiment_id"] == "1dn_base")
base_3dn = next(e["total_time_sec"] for e in experiments if e["experiment_id"] == "3dn_base")
opt_1dn  = next(e["total_time_sec"] for e in experiments if e["experiment_id"] == "1dn_opt")
opt_3dn  = next(e["total_time_sec"] for e in experiments if e["experiment_id"] == "3dn_opt")

speedup_labels = ["1DN опт. vs базовый", "3DN опт. vs базовый", "3DN базовый vs 1DN базовый"]
speedup_values = [
    base_1dn / opt_1dn,
    base_3dn / opt_3dn,
    base_1dn / base_3dn,
]

fig, ax = plt.subplots(figsize=(9, 5))
bars = ax.bar(speedup_labels, speedup_values,
              color=["#6acc65", "#b47cc7", "#d65f5f"], width=0.4, edgecolor="white")
ax.axhline(y=1.0, color="gray", linestyle="--", linewidth=1.2, label="нет эффекта (1.0x)")
for bar, val in zip(bars, speedup_values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
            f"{val:.2f}x", ha="center", fontweight="bold", fontsize=13)
ax.set_ylabel("Ускорение (раз)", fontsize=12)
ax.set_title("Ускорение по времени относительно базового (baseline) эксперимента", fontsize=13, fontweight="bold")
ax.set_ylim(0, max(speedup_values) * 1.3)
ax.yaxis.grid(True, linestyle="--", alpha=0.5)
ax.set_axisbelow(True)
ax.legend()
plt.tight_layout()
plt.savefig(os.path.join(CHARTS_DIR, "chart_speedup.png"), dpi=150)
plt.close()
print("Сохранён: chart_speedup.png")

print(f"\nВсе графики в папке: {CHARTS_DIR}/")