# finx-option-data
Fetch, store, manipulate, and store option data from vendors.

## Quick start

```sh
cd ../finx-option-pricer && pip install -e . && cd finx-option-data
pip install -e .
```

## ReWrite Todos

- [x] setup.py file and python folder structure for `finx_option_data`
- [ ] move methods into finx_option_data
    - [ ] handler.py 
    - [ ] post_process_handler.py
    - [ ] handler_download_data.py
    - [ ] configure.py
- [ ] decide what to do with methods in cli.py
- [ ] separate logic into sections, 
    - AWS CRUD operations
    - TDA API operations
    - finx_option_data intelligent operations
    - utility ops (glue)
- [ ] create better setup instructions
- [ ] setup integration tests
