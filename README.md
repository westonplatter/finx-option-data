# finx-option-data
Finx Focus = ingest, enrich

Fetch, store, and warehouse Option Data

```
cd ../finx-option-pricer && pip install -e . && cd finx-option-data
pip install -e .
```

## Quick start

@TODO - update to describe how to setup configurations in AWS Secret Param store vs .env.prod file.

1. Copy over sample `.env.sample` to `.env.prod`. See `.env. file` section.
2. Create a TDA client id. Add value to `.env`
3. Generate TDA credentials file, `make config.tda`
4. Configure DB, `make config.db` # add PostgresDB password to the `.env.prod` file
5. Build docker container, `make docker.build`
6. Run the fetch, `aws-vault exec ${profile_name} -- make fetch`
7. Run the move, `aws-vault exec ${profile_name} -- make move`


## Deployment 
1. `aws-vault exec ${profile_name} -- make deploy.prod`


## .env file
Expected values in the `.env.{stage}` file. `prod` is the default stage.

```
POSTGRES_CONNECTION_STRING=postgresql://username:password@host:5432/db_name
TDA_CLIENT_ID=ABC123-somethingelse
BUCKET_NAME=your-bucket-name
TDA_REDIRECT_URL=http://localhost
DISCORD_CHANNEL_URL="https://discord.com/api/webhooks/123/abc-xyz"
```

## ReWrite Todos

- [ ] setup.py file and python folder structure for `finx_option_data`
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
