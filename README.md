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

## TODOs
- document this, `aws_profile_session ${profile_name}`

## Getting setup
0. Copy over sample `.env.sample` to `.env`
1. Create a TDA client id. Add value to `.env`
2. Generate TDA creds file, `make config.tda`


- Provide a valid PostgreSQL connection string in `.env`, `POSTGRES_CONNECTION_STRING`
- Provide a valid TD Ameritrade Client Id in `.env`, `TDA_CLIENT_ID`. See (`tda-api`)[https://tda-api.readthedocs.io/en/latest/] for more details.
- Provide an S3 bucket name in `.env`, `BUCKET_NAME`. Ensure the Lambda was read/write permissions.
