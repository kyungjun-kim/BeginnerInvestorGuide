FROM apache/airflow:2.7.3-python3.9

# Install PyTorch with no-cache-dir option to reduce image size
RUN pip install --no-cache-dir torch transformers

# After enter this command 

# VERSION=0.0.1
# docker build . -t airflow_docker:${VERSION}