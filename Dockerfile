FROM apache/airflow:2.10.3

USER root

# Install required packages
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         wget \
         curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Hadoop
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.3/hadoop-3.3.3.tar.gz \
  && tar -xzf hadoop-3.3.3.tar.gz -C /opt/ \
  && ln -s /opt/hadoop-3.3.3 /opt/hadoop \
  && rm hadoop-3.3.3.tar.gz

# Install Spark (updated to 3.4.4)
RUN wget -q https://downloads.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz \
  && tar -xzf spark-3.4.4-bin-hadoop3.tgz -C /opt/ \
  && ln -s /opt/spark-3.4.4-bin-hadoop3 /opt/spark \
  && rm spark-3.4.4-bin-hadoop3.tgz

# Set environment variables for Hadoop and Spark
USER airflow
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV YARN_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH="${HADOOP_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

# Install Airflow and providers
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3
