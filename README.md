# redaptive-python-template

TODO: Replace this README with your project specific README

This is a template project to be used to bootstrap the setup of new Python projects intended to be deployed as lambdas on AWS. 

This follows the coding standards and style guidelines [published here](https://redaptiveinc.atlassian.net/wiki/spaces/EW/pages/2238971905/Python+Coding+Standards+Style+Guidelines). 

## Setup and Initialization

1. Create a new repository from this template ([instructions here](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/creating-a-repository-on-github/creating-a-repository-from-a-template#creating-a-repository-from-a-template))
2. Run:
    ```
    pip install -r requirements.txt
    pre-commit install
    ```
3. Replace the template with your code

## Running Tests
Ensure that any needed environment variables that would be set by CircleCI are set, then:
`python -m unittest discover`

## Running locally using AWS SAM
First install the AWS SAM CLI and Docker (if not already installed). On a Mac with Homebrew:
```
brew install --cask docker
brew tap aws/tap
brew install aws-sam-cli
```
Build the container (each time the code or dependencies are updated):
```
sam build -u
```
Run the function locally with:
```
sam local invoke RedaptivePythonLambdaTemplateFunction -e events/sample.json -n config/local.json
```