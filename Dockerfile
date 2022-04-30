FROM public.ecr.aws/lambda/python:3.8

COPY requirements/requirements.txt ./
RUN pip install install -r requirements/requirements.txt

COPY handler.py ./
COPY handler_post_process.py ./
COPY helpers.py ./
COPY .env.prod ./
COPY tda_api_creds.json ./

# You can overwrite command in `serverless.yml` template
CMD ["handler.handler_fetch_data"]
