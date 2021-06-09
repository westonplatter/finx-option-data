FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt ./
RUN pip install install -r requirements.txt

COPY handler.py ./
COPY .env ./
COPY tda_api_creds.json /tmp/tda_api_creds.json

# You can overwrite command in `serverless.yml` template
CMD ["handler.handler_fetch_data"]