# finx-option-data
Ingest, Enrich, and Store Options data from Polygon

# Quick setup
This repo is designed in conjunction with https://github.com/westonplatter/finx-option-pricer.
Therefore, you'll need both FOD (finx-option-data) and FOP (finx-option-pricer)
```
git clone fop
git clone fod
cd fod
cd ../finx-option-pricer && pip install -e . && cd ../finx-option-data
pip install -e .
```
## Quick start
1. Copy over sample `.env.sample` to `.env.prod`. See `.env. file` section.
2. Run alembic migrations. `cd db && alembic upgrade head && ..`
3. Run files in scripts

## TODOs
- [ ] create better setup instructions
- [ ] setup integration tests