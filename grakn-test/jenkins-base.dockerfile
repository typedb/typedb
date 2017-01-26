FROM ubuntu:16.04
RUN locale-gen en_GB.UTF-8
ENV LANG en_GB.UTF-8
RUN apt-get update
RUN apt-get install -y software-properties-common debconf-utils curl
RUN add-apt-repository ppa:webupd8team/java
RUN apt-get update
RUN echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
RUN echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
RUN apt-get install -y lsof maven oracle-java8-installer

RUN curl -sL https://deb.nodesource.com/setup_6.x -o nodesource_setup.sh && \
    bash nodesource_setup.sh && \
    apt-get install -u nodejs

COPY .m2 /root/.m2/

RUN rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/oracle-jdk8-installer

CMD ["bash"]
