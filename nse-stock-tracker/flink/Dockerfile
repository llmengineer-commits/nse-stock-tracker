FROM flink:1.18-python3

# Install Python dependencies for the Flink job
# clickhouse-connect uses HTTP (port 8123) — no JDBC driver needed
COPY requirements.txt /opt/flink/requirements.txt
RUN pip install --no-cache-dir -r /opt/flink/requirements.txt

# Download the Flink Kafka connector JAR (needed for the Kafka source)
RUN wget -q -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Copy the job script
COPY nse_flink_job.py /opt/flink/jobs/

WORKDIR /opt/flink
