FROM public.ecr.aws/lambda/python:3.11

# Set up environment variables for Poetry
ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV VIRTUAL_ENV="/opt/python"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install system dependencies and Poetry
RUN yum install -y gcc gcc-c++ make libffi-devel python3-devel \
    && curl -sSL https://install.python-poetry.org | python3 -

# Set working directory to /var/task (default Lambda working directory)
WORKDIR /var/task

# Copy only necessary files for Poetry to install dependencies
COPY pyproject.toml README.md ./

# Install dependencies with Poetry, installing them directly to /opt/python
RUN poetry install --no-interaction --without dev --install-option="--prefix=/opt/python"

# Copy the rest of your source code to the container
COPY src .

# Set Python path to ensure Lambda can find the installed packages
ENV PYTHONPATH=/opt/python

# Print the directory structure for debugging
RUN echo "Contents of /var/task: " && ls -R /var/task && echo "Contents of /opt/python: " && ls -R /opt/python

# Lambda function handler
CMD ["main.handler"]
