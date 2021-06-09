# static
DOCKER_TAG ?= serverless-finx-option-data-dev:appimage
AWS_REGION ?= us-east-1

# dynamic
# GET_AWS_ACCOUNT_ID := $$(aws sts get-caller-identity | jq -r .Account)

docker.build:
	docker build -t ${DOCKER_TAG} .

docker.run:
	docker run -p 9000:8080 ${DOCKER_TAG}

local.invoke.fetch:
	curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'   

# docker.run.store:
# 	docker run \
# 		--env AWS_ACCESS_KEY_ID=$$AWS_ACCESS_KEY_ID \
# 		--env AWS_SECRET_ACCESS_KEY=$$AWS_SECRET_ACCESS_KEY \
# 		--env AWS_SESSION_TOKEN=$$AWS_SESSION_TOKEN \
# 		--env AWS_SESSION_EXPIRATION=$$AWS_SESSION_EXPIRATION \
# 		-it ${DOCKER_TAG}

deploy:
	sls deploy

deploy.prod:
	sls deploy --stage prod