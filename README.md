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