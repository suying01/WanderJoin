# 1. Using the modern Python 3.12 base (Amazon Linux 2023)
FROM public.ecr.aws/lambda/python:3.12

# 2. Use 'dnf' instead of 'yum' for AL2023
RUN dnf install -y gcc gcc-c++ make && \
    dnf clean all

# 3. Upgrade pip and install requirements
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 4. Pre-bake the DuckDB extension into the /tmp folder of the image
# This prevents the Lambda from downloading it every single time
RUN python3 -c "import duckdb; con = duckdb.connect(); con.execute('SET home_directory=\"/tmp\"; INSTALL httpfs;')"

# 5. Copy your function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the entry point for AWS Lambda
CMD [ "lambda_function.handler" ]