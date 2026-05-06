FROM apache/airflow:2.10.4-python3.11

# Packages needed by pipeline tasks.
# Heavy ML deps (torch, open-clip-torch, sentence-transformers) are added
# once the ML task modules are implemented.
RUN pip install --no-cache-dir \
    "psycopg[binary]>=3.1" \
    "pgvector>=0.3" \
    "boto3>=1.35"
