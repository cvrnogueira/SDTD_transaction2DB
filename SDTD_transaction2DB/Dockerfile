FROM centos:latest

RUN yum install -y jq

ADD /target/scala*/*-assembly*.jar /opt/app.jar
ADD flink-upload.sh /opt/flink-upload.sh

CMD /bin/bash /opt/flink-upload.sh