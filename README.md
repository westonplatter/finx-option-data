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

# makefile with docker
aws-vault exec ${profile_name} && docker.run.store
```

## .env file
aws_profile_session ${profile_name}