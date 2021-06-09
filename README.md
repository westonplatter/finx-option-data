# finx-option-data
Fetch, store, and warehouse Option Data

## docker build
```
aws_profile_session ${profile_name}
make docker.build
```

## fetching data
```
# local python
python cli.py do-fetch-data

# makefile with docker
make docker.run.fetch
```

## storing data
```
# local python
aws-vault exec ${profile_name} -- python cli.py do-move-data-to-s3

# makefile with docker
aws-vault exec ${profile_name} && docker.run.store
```

## .env file



