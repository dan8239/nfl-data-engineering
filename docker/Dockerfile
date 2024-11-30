FROM public.ecr.aws/lambda/python:3.11

# Set up environment variables for Poetry
ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true

# Install system dependencies and Poetry
RUN yum install -y gcc gcc-c++ make libffi-devel python3-devel \
    && curl -sSL https://install.python-poetry.org | python3 -

# Set working directory to /var/task (default Lambda working directory)
WORKDIR /var/task

# Copy only necessary files for Poetry to install dependencies
COPY pyproject.toml README.md ./

# Set the VIRTUAL_ENV to /opt/python so Poetry installs dependencies directly there
ENV VIRTUAL_ENV="/opt/python"

# Install dependencies with Poetry, excluding dev dependencies
RUN poetry install --no-interaction --without dev

# Copy the rest of your source code to the container
COPY src .

# Set Python path to ensure Lambda can find the installed packages
ENV PYTHONPATH=/opt/python

# Print the directory structure for debugging
RUN echo "Contents of /var/task: " && ls -R /var/task && echo "Contents of /opt/python: " && ls -R /opt/python

# Lambda function handler
CMD ["main.handler"]
