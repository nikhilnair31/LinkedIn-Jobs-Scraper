# FROM public.ecr.aws/lambda/python:3.7
FROM umihico/aws-lambda-selenium-python:latest

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ .

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "scraper_handler.handler" ]