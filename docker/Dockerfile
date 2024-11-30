FROM public.ecr.aws/lambda/python:3.11-arm64

ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true

RUN yum install -y gcc gcc-c++ make libffi-devel python3-devel \
    && curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /var/task

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-dev --no-interaction --no-ansi

COPY src .

CMD ["src.main.handler"]