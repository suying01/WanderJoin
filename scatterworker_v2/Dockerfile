FROM public.ecr.aws/lambda/python:3.12

# Install dependencies
RUN pip install duckdb numpy boto3

# Copy the handler
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD [ "lambda_function.handler" ]