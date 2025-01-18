FROM apache/hive:3.1.3

USER root

# Install netcat for health checks
RUN apt-get update && \
    apt-get install -y netcat && \
    rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /opt/hive/lib/ /opt/hive/conf/

# Copy specific JAR files
COPY jars/postgresql-42.2.18.jar /opt/hive/lib/
COPY jars/hadoop-aws-3.2.0.jar /opt/hive/lib/
COPY jars/aws-java-sdk-bundle-1.11.375.jar /opt/hive/lib/

# Remove conflicting slf4j library
RUN rm -f /opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-*.jar

# Copy Hive configuration
COPY conf/hive-site.xml /opt/hive/conf/

# Copy entrypoint script
COPY ./entrypoint.sh /opt/hive/entrypoint.sh
RUN chmod +x /opt/hive/entrypoint.sh

# Set proper permissions
RUN chown -R hive:hive /opt/hive

USER hive

EXPOSE 9083

ENTRYPOINT ["/opt/hive/entrypoint.sh"]
