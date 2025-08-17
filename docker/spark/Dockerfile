FROM bitnami/spark:3.4.1

USER root

# Cài đặt Python packages cần thiết
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    py4j==0.10.9.7 \
    pandas \
    pyarrow \
    boto3

USER 1001
