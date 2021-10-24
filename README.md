# finx-option-data
Fetch, store, and warehouse Option Data

## docker build
```sh
make docker.build
```

## fetching data
```sh
# local python
python cli.py do-fetch-data
```

## storing data
```sh
# local python
aws-vault exec ${profile_name} -- python cli.py do-move-data-to-s3
```

## .env file
Expected values in the `.env` file
```
POSTGRES_CONNECTION_STRING=postgresql://username:password@host:5432/db_name
TDA_CLIENT_ID=ABC123-somethingelse
BUCKET_NAME=your-bucket-name
TDA_REDIRECT_URL=http://localhost
```

## Getting setup
1. Copy over sample `.env.sample` to `.env`
2. Create a TDA client id. Add value to `.env`
3. Generate TDA creds file, `make config.tda`
4. Run the fetch, `make fetch`
5. Run the move, `aws-vault exec ${profile_name} -- make move`


## Deployment 
1. `aws-vault exec ${profile_name} -- make deploy.prod`
