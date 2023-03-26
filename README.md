# finx-option-data
Fetch, store, manipulate, and store option data from vendors.

**WARNING - software is in alpha status. Expect the API to change.**

## Vendors
- [x] Polygon


# Quick setup
This repo is designed in conjunction with https://github.com/westonplatter/finx-option-pricer.
Therefore, you'll need both FOD (finx-option-data) and FOP (finx-option-pricer)
```
# install option pricer
git clone git@github.com:westonplatter/finx-option-pricer.git
cd finx-option-pricer
pip install -e .
cd ..

# install this repo, finx-option-data
git clone git@github.com:westonplatter/finx-option-data.git
cd finx-option-data
cd ../finx-option-data
pip install -e .
```

## Quick start
1. Copy over sample `.env.sample` to `.env.prod`
2. Run example file, `python finx_option_data/x_fetch.py`


## ReWrite Todos
- [x] setup.py file and python folder structure for `finx_option_data`
- [ ] separate logic into sections, 
  - [ ] polygon API operations
  - [ ] AWS CRUD operations
  - [ ] finx_option_data intelligent operations
  - [ ] utility ops (glue)