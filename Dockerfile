FROM reedelk/reedelk-runtime-ce:1.0.1-SNAPSHOT
COPY target/*.jar /opt/reedelk-runtime/modules
CMD runtime-start
