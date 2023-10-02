FROM public.ecr.aws/e0a2p4z8/lambda-layer-read-from-ssm-python:1.0.0 as layer
FROM public.ecr.aws/lambda/python:3.9
COPY --from=layer /opt/fetch_ssm_parameters /opt/fetch_ssm_parameters
ENV AWS_LAMBDA_EXEC_WRAPPER=/opt/fetch_ssm_parameters/fetch-ssm-parameters.py

RUN yum -y install git
ARG GITHUB_READ_TOKEN

COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY src   .
COPY test  .

CMD ["main.handler"]