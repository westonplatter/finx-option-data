.DEFAULT_GOAL := help

# static
DOCKER_TAG ?= serverless-finx-option-data-dev:appimage
AWS_REGION ?= us-east-1

# dynamic
# GET_AWS_ACCOUNT_ID := $$(aws sts get-caller-identity | jq -r .Account)

help:
	echo "todo"

config.tda:
	python cli.py gen-tda-creds

fetch:
	python cli.py do-fetch-data

move:
	python cli.py do-move-data-to-s3


docker.build:
	docker build -t ${DOCKER_TAG} .

docker.local.fetch:
	docker run -p 9000:8080 ${DOCKER_TAG} handler.handler_fetch_data
	
docker.local.move:
	docker run \
		-p 9000:8080 \
		--env AWS_ACCESS_KEY_ID=$$AWS_ACCESS_KEY_ID \
		--env AWS_SECRET_ACCESS_KEY=$$AWS_SECRET_ACCESS_KEY \
		--env AWS_SESSION_TOKEN=$$AWS_SESSION_TOKEN \
		--env AWS_SESSION_EXPIRATION=$$AWS_SESSION_EXPIRATION \
		${DOCKER_TAG} \
		handler.handler_move_data_to_s3

docker.invoke:
	curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'


deploy:
	sls deploy --stage dev --verbose

deploy.prod:
	sls deploy --stage prod --verbose


aws.invoke.prod.fetch:
	sls invoke --stage prod --function fetch

aws.invoke.prod.move:
	sls invoke --stage prod --function move
