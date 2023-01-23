FROM ubuntu:jammy

MAINTAINER Helena Schmidt

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8

RUN apt-get -y update -qq && \
    apt-get -y install locales && \
    locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8 && \
    apt-get install -y python3-pip python3-pandas
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/* /var/tmp/*


RUN mkdir -p /app
RUN mkdir -p /app/data

WORKDIR /app

COPY requirements.txt /app/
RUN pip3 install -r requirements.txt

COPY app.py /app/
COPY controller.py /app/
COPY startup.sh /app/

EXPOSE 5000

ENTRYPOINT ["/bin/bash", "/app/startup.sh"]
