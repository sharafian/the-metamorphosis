FROM ubuntu
RUN apt-get update && apt-get install -yq \
    default-jre \
    vim \
    zookeeperd
RUN apt-get install -yq wget
RUN wget "http://www-eu.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz" -O kafka.tgz
RUN tar -xzvf kafka.tgz --strip 1
# Replace shell with bash so we can source files
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

# install nodejs using nvm
ENV NVM_DIR /usr/local/nvm
ENV NODE_VERSION 7.7.1

RUN wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.1/install.sh | bash \
    && source $NVM_DIR/nvm.sh \
    && nvm install $NODE_VERSION \
    && nvm alias default $NODE_VERSION \
    && nvm use default

ENV NODE_PATH $NVM_DIR/v$NODE_VERSION/lib/node_modules
ENV PATH      $NVM_DIR/v$NODE_VERSION/bin:$PATH

ADD . .
RUN source $NVM_DIR/nvm.sh && npm install
ENV PORT 3010
EXPOSE 3010
RUN echo 127.0.0.1\ moby >> /etc/hosts
CMD sh ./start.sh
