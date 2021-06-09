# static
CONDA_ENV ?= finx_option_data
DOCKER_TAG ?= finx_option_data
AWS_REGION ?= us-east-1
ECR_REPO_NAME ?= qdl-prod

# dynamic
GET_AWS_ACCOUNT_ID := $$(aws sts get-caller-identity | jq -r .Account)

test:
	@pytest -s .

env.create:
	@conda create -y -n ${CONDA_ENV} python=3.7

env.update:
	@conda env update -n ${CONDA_ENV} -f environment.yml


docker.build:
	docker build -t ${DOCKER_TAG} .

docker.run:
	docker run -p 9000:8080 ${DOCKER_TAG}

docker.run.bash:
	docker run -it ${DOCKER_TAG} /bin/bash

docker.run.fetch:
	docker run -i -t ${DOCKER_TAG}

docker.run.store:
	docker run \
		--env AWS_ACCESS_KEY_ID=$$AWS_ACCESS_KEY_ID \
		--env AWS_SECRET_ACCESS_KEY=$$AWS_SECRET_ACCESS_KEY \
		--env AWS_SESSION_TOKEN=$$AWS_SESSION_TOKEN \
		--env AWS_SESSION_EXPIRATION=$$AWS_SESSION_EXPIRATION \
		-it ${DOCKER_TAG}

ecr.login:
	aws ecr \
		get-login-password \
		--region ${AWS_REGION} \
		| \
		docker login \
		--username AWS \
		--password-stdin ${GET_AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
	
ecr.push:
	docker tag ${DOCKER_TAG}:latest ${GET_AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:latest
	docker push ${GET_AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:latest