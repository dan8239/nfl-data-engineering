FROM public.ecr.aws/lambda/python:3.13

ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true

RUN dnf install -y gcc gcc-c++ make libffi-devel python3-devel \
    && curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /var/task

COPY pyproject.toml README.md ./

RUN poetry install --no-interaction --no-root

COPY src .

RUN mkdir -p /opt/python && cp -r .venv/lib/python3.13/site-packages/* /opt/python/

ENV PYTHONPATH=/opt/python

RUN echo "Contents of /var/task: " && ls -R /var/task && echo "Contents of /opt/python: " && ls -R /opt/python

CMD ["main.handler"]