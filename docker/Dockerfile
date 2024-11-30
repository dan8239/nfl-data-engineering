FROM public.ecr.aws/lambda/python:3.11

# Set up environment variables for Poetry
ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true

# Install system dependencies
RUN yum install -y gcc gcc-c++ make libffi-devel python3-devel \
    && curl -sSL https://install.python-poetry.org | python3 -

# Set working directory
WORKDIR /var/task

# Copy only necessary files for Poetry to install dependencies
COPY pyproject.toml README.md ./

# Install dependencies with Poetry (without creating a virtualenv)
RUN poetry install --no-interaction --no-root --no-dev --prefer-poetry

# Copy your source code to the container
COPY src .

# Ensure Poetry packages are accessible to Lambda by copying them to /opt/python
RUN mkdir -p /opt/python && cp -r .venv/lib/python3.11/site-packages/* /opt/python/

# Ensure the correct path is set for Python to find the installed packages
ENV PYTHONPATH=/opt/python

# Print contents to verify everything is in place
RUN echo "Contents of /var/task: " && ls -R /var/task && echo "Contents of /opt/python: " && ls -R /opt/python

# Specify the Lambda handler
CMD ["main.handler"]
