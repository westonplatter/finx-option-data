# static
DOCKER_TAG ?= serverless-finx-option-data-dev:appimage
AWS_REGION ?= us-east-1

# dynamic
# GET_AWS_ACCOUNT_ID := $$(aws sts get-caller-identity | jq -r .Account)

config.tda:
	python cli.py gen-tda-creds


docker.build:
	docker build -t ${DOCKER_TAG} .

docker.run.fetch:
	docker run -p 9000:8080 ${DOCKER_TAG} handler.handler_fetch_data
	
docker.run.move:
	docker run \
		-p 9000:8080 \
		--env AWS_ACCESS_KEY_ID=$$AWS_ACCESS_KEY_ID \
		--env AWS_SECRET_ACCESS_KEY=$$AWS_SECRET_ACCESS_KEY \
		--env AWS_SESSION_TOKEN=$$AWS_SESSION_TOKEN \
		--env AWS_SESSION_EXPIRATION=$$AWS_SESSION_EXPIRATION \
		${DOCKER_TAG} \
		handler.handler_move_data_to_s3

local.invoke:
	curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'

deploy:
	sls deploy

deploy.prod:
	sls deploy --stage prod
