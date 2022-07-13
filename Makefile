# static
DOCKER_TAG ?= serverless-finx-option-data-dev:appimage
AWS_REGION ?= us-east-1

test:
	pytest .

env.update:
	pip install -r requirements/requirements.txt


#------------------------------------------------------------------------------
# Actions - local, docker
#------------------------------------------------------------------------------

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



#------------------------------------------------------------------------------
# Actions - prod
#------------------------------------------------------------------------------

aws.invoke.prod.fetch:
	sls invoke --stage prod --function fetch

aws.invoke.prod.move:
	sls invoke --stage prod --function move

aws.invoke.prod.post_process_yesterday:
	sls invoke --stage prod --function post_process_yesterday



#------------------------------------------------------------------------------
# Deployment
#------------------------------------------------------------------------------

deploy:
	sls deploy --stage dev --verbose

deploy.prod:
	sls deploy --stage prod --verbose


#------------------------------------------------------------------------------
# Backup
#------------------------------------------------------------------------------

backup:
	cp analysis*.ipynb ../forecasters/fod
