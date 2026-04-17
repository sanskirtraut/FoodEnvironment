
# Food Environment Tracker — Georgia

This is a food environment tracker that calculates the swamp index of a county or zip code for Georgia. The Food "Swamp" refers to the excess of fast food in an area that has very 
little fresh food establishments like a supermarket. This project is to highlight the growing problem of excess processed food establishments that outnumber fresh food establishments.

## Setup

```bash
pip install -r requirements.txt
python init_db.py   # parses PLACES CSV + OSM JSONs → food_env.db (~2–3 min)
python app.py       # http://localhost:5000
```

## What it does

- **Search (Read)** by ZIP / name — side-by-side supermarkets vs. fast food
- **Swamp Severity Calculator** — pick a census tract + radius, computes the RFEI (Fast Food ÷ Supermarket) and joins CDC obesity rate for that tract
- **Analytics** — aggregation: establishments grouped by census tract
- **Manage** — insert, update (fresh-food flag), delete

## Schema

`Census_Tract`, `Category_Type`, `Obesity_Statistic`, `Food_Establishment`, `Competes_With` — 
