# Food Environment Tracker — Georgia


## Setup
Download the USDA data from this link
https://data.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-County-Data-20/swc5-untb/about_data

Click export as csv file (around 850 mbs+) 

Make sure to place this file in FoodEnvironment/data folder beforehand
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
