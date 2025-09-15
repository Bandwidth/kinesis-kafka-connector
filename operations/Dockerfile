FROM confluentinc/cp-server-connect:7.5.0

USER root

# Install JDK, git and maven for building the connector
RUN yum update -y && yum install -y git maven java-11-openjdk-devel

# Set JAVA_HOME to point to the JDK
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk

# Copy the current repo source code into the image
WORKDIR /tmp/kinesis-kafka-connector
COPY . .

# Build the Kinesis connector
RUN mvn clean package -DskipTests -q

# Create plugin directory and copy the built JAR
RUN mkdir -p /usr/share/java/kinesis-connector
RUN find target/ -name "*.jar" -exec cp {} /usr/share/java/kinesis-connector/ \;

# Clean up build dependencies but keep Java runtime
RUN yum remove -y git maven && \
    yum install -y java-11-openjdk && \
    yum clean all && \
    rm -rf /tmp/kinesis-kafka-connector

USER appuser
WORKDIR /
