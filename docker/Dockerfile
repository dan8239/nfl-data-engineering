FROM public.ecr.aws/lambda/python:3.11

ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"

RUN yum install -y gcc gcc-c++ make libffi-devel python3-devel \
    && curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /var/task

COPY pyproject.toml README.md ./
RUN poetry install --no-interaction --only main

COPY src .

CMD ["main.handler"]
