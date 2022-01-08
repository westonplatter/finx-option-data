# finx-option-data
Fetch, store, and warehouse Option Data

## Quick start

1. Copy over sample `.env.sample` to `.env`. See `.env file` section.
2. Create a TDA client id. Add value to `.env`
3. Generate TDA credentials file, `make config.tda`
4. Configure DB, `make config.db`
5. Build docker container, `make docker.build`
6. Run the fetch, `aws-vault exec ${profile_name} -- make fetch`
7. Run the move, `aws-vault exec ${profile_name} -- make move`


## Deployment 
1. `aws-vault exec ${profile_name} -- make deploy.prod`


## .env file
Expected values in the `.env.{stage}` file

```
POSTGRES_CONNECTION_STRING=postgresql://username:password@host:5432/db_name
TDA_CLIENT_ID=ABC123-somethingelse
BUCKET_NAME=your-bucket-name
TDA_REDIRECT_URL=http://localhost
DISCORD_CHANNEL_URL="https://discord.com/api/webhooks/123/abc-xyz"
```

## Changelog
Created by running,

```sh
make changelog
```

And requires having [`git-chglog`](https://github.com/git-chglog/git-chglog) installed.
