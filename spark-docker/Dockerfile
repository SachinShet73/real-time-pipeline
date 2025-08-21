FROM bitnami/spark:latest

USER root

# Install Python dependencies
RUN pip install cassandra-driver kafka-python

# Copy the Spark streaming script
COPY spark_stream.py /opt/spark_stream.py

WORKDIR /opt

CMD ["python", "spark_stream.py"]