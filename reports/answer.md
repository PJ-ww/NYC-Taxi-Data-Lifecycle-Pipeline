# Answer: most used taxi type, Jan–Jun 2024

**High-volume FHV (Uber, Lyft)** (`fhvhv`) was the most used, with
**120,864,668 trips** (80.8% of all trips),
about **5.9×** the next type, Yellow taxi (20,331,644 trips).
It was the highest in every month from January to June.

Metric: number of trips (one row = one trip) kept after cleaning in `src/curate.py`.

| rank | label | total_trips | share_pct | avg_trips_per_day |
|---|---|---|---|---|
| 1 | High-volume FHV (Uber, Lyft) | 120,864,668 | 80.76 | 664,092 |
| 2 | Yellow taxi | 20,331,644 | 13.59 | 111,712 |
| 3 | FHV (other bases) | 8,119,228 | 5.43 | 44,611 |
| 4 | Green taxi | 339,763 | 0.23 | 1,867 |

Figures: `fig_total_trips.png`, `fig_monthly_trips.png`.
Monthly breakdown: `monthly_trips.csv`.
