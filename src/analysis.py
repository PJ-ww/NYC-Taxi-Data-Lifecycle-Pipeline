"""
Answer the question: Which taxi type was the most used in the first half of 2024?

Metric: number of trips (rows kept after cleaning) per taxi type, Jan-Jun 2024.
Input : data/curated/daily_trips_all.parquet   (built by src/curate.py)
Checks: curated totals must equal kept_rows in reports/curation_log.csv
Output: reports/answer_summary.csv   totals, share, rank per taxi type
        reports/monthly_trips.csv    trips per month per taxi type
        reports/fig_total_trips.png
        reports/fig_monthly_trips.png
        reports/answer.md            the answer in words, with numbers
"""
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd

from config.datasets import CURATED_DIR, REPORTS_DIR, DATASETS

CURATED_FILE = CURATED_DIR / "daily_trips_all.parquet"
LOG_FILE = REPORTS_DIR / "curation_log.csv"

LABELS = {
    "fhvhv": "High-volume FHV (Uber, Lyft)",
    "yellow": "Yellow taxi",
    "fhv": "FHV (other bases)",
    "green": "Green taxi",
}
# colour follows the taxi type, never its rank
COLORS = {"fhvhv": "#2a78d6", "yellow": "#eda100", "green": "#008300", "fhv": "#e34948"}
INK, INK_2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"


def millions(x, _=None):
    if x == 0:
        return "0"
    return f"{x / 1e6:.0f}M" if x >= 1e6 else f"{x / 1e3:.0f}k"


def load():
    if not CURATED_FILE.exists():
        sys.exit(f"Missing {CURATED_FILE}. Run: python -m src.curate")
    df = pd.read_parquet(CURATED_FILE)
    df["trip_date"] = pd.to_datetime(df["trip_date"])
    df["month"] = df["trip_date"].dt.to_period("M").astype(str)
    return df


def check_completeness(df):
    """Every expected source file is present, and totals match the curation log."""
    expected = {d["filename"] for d in DATASETS}
    missing = expected - set(df["source_file"])
    if missing:
        sys.exit(f"Curated data incomplete, missing: {sorted(missing)}")

    if LOG_FILE.exists():
        log = pd.read_csv(LOG_FILE).set_index("filename")["kept_rows"]
        curated = df.groupby("source_file")["trips"].sum()
        diff = (curated - log.reindex(curated.index)).abs()
        if (diff > 0).any():
            sys.exit(f"Curated totals do not match curation_log:\n{diff[diff > 0]}")
        print("Check passed: curated totals match curation_log.csv for all files")


def summarise(df):
    summary = (
        df.groupby("taxi_type")
        .agg(total_trips=("trips", "sum"),
             total_miles=("total_miles", lambda s: s.sum(min_count=1)),
             days=("trip_date", "nunique"))
        .sort_values("total_trips", ascending=False)
    )
    summary["avg_trips_per_day"] = (summary["total_trips"] / summary["days"]).round(0)
    summary["share_pct"] = (100 * summary["total_trips"] / summary["total_trips"].sum()).round(2)
    summary["rank"] = range(1, len(summary) + 1)
    summary.insert(0, "label", summary.index.map(LABELS))

    monthly = df.pivot_table(index="month", columns="taxi_type", values="trips", aggfunc="sum")
    monthly = monthly[summary.index]  # same order as the ranking
    return summary.reset_index(), monthly


def style(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=9)


def plot_totals(summary):
    s = summary.sort_values("total_trips")
    fig, ax = plt.subplots(figsize=(8, 3.6))
    ax.barh(s["label"], s["total_trips"], color=[COLORS[t] for t in s["taxi_type"]], height=0.6)
    for y, (v, pct) in enumerate(zip(s["total_trips"], s["share_pct"])):
        ax.text(v, y, f"  {v:,.0f} ({pct:.1f}%)", va="center", fontsize=9, color=INK)
    ax.xaxis.set_major_formatter(FuncFormatter(millions))
    ax.set_xlim(0, s["total_trips"].max() * 1.35)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    style(ax)
    ax.set_title("Total trips by taxi type, Jan–Jun 2024", loc="left", fontsize=12, color=INK)
    fig.text(0.01, 0.01, "Source: NYC TLC trip records, cleaned (src/curate.py)", fontsize=8, color=INK_2)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    path = REPORTS_DIR / "fig_total_trips.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_monthly(monthly):
    fig, ax = plt.subplots(figsize=(8, 4.2))
    x = range(len(monthly.index))
    for t in monthly.columns:
        ax.plot(x, monthly[t], color=COLORS[t], linewidth=2, marker="o", markersize=5, label=LABELS[t])
        ax.text(len(x) - 1 + 0.1, monthly[t].iloc[-1], LABELS[t], va="center", fontsize=9, color=INK)
    ax.set_yscale("log")  # green is ~400x smaller than fhvhv; log keeps every line readable
    ax.yaxis.set_major_formatter(FuncFormatter(millions))
    ax.set_xticks(list(x), monthly.index)
    ax.set_xlim(-0.2, len(x) + 1.6)
    ax.grid(axis="y", color=GRID, linewidth=0.8, which="major")
    ax.set_axisbelow(True)
    style(ax)
    ax.legend(loc="upper left", bbox_to_anchor=(0, -0.1), ncol=4, frameon=False, fontsize=8)
    ax.set_title("Monthly trips by taxi type, 2024 (log scale)", loc="left", fontsize=12, color=INK)
    fig.tight_layout()
    path = REPORTS_DIR / "fig_monthly_trips.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def md_table(df):
    """Markdown table without extra dependencies."""
    head = "| " + " | ".join(df.columns) + " |"
    sep = "|" + "---|" * len(df.columns)
    rows = ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join([head, sep, *rows])


def write_answer(summary, monthly):
    top, second = summary.iloc[0], summary.iloc[1]
    ratio = top["total_trips"] / second["total_trips"]
    leads_every_month = (monthly.idxmax(axis=1) == top["taxi_type"]).all()

    table = summary[["rank", "label", "total_trips", "share_pct", "avg_trips_per_day"]].copy()
    table["total_trips"] = table["total_trips"].map("{:,.0f}".format)
    table["avg_trips_per_day"] = table["avg_trips_per_day"].map("{:,.0f}".format)

    text = f"""# Answer: most used taxi type, Jan–Jun 2024

**{top['label']}** (`{top['taxi_type']}`) was the most used, with
**{top['total_trips']:,.0f} trips** ({top['share_pct']:.1f}% of all trips),
about **{ratio:.1f}×** the next type, {second['label']} ({second['total_trips']:,.0f} trips).
It {'was the highest in every month' if leads_every_month else 'was not the highest in every month'} from January to June.

Metric: number of trips (one row = one trip) kept after cleaning in `src/curate.py`.

{md_table(table)}

Figures: `fig_total_trips.png`, `fig_monthly_trips.png`.
Monthly breakdown: `monthly_trips.csv`.
"""
    path = REPORTS_DIR / "answer.md"
    path.write_text(text, encoding="utf-8")
    return path, text


def run_analysis():
    df = load()
    check_completeness(df)
    summary, monthly = summarise(df)

    summary.to_csv(REPORTS_DIR / "answer_summary.csv", index=False)
    monthly.to_csv(REPORTS_DIR / "monthly_trips.csv")
    plot_totals(summary)
    plot_monthly(monthly)
    _, text = write_answer(summary, monthly)

    print(text)
    print("Saved to reports/: answer.md, answer_summary.csv, monthly_trips.csv, "
          "fig_total_trips.png, fig_monthly_trips.png")


if __name__ == "__main__":
    run_analysis()