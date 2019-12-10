FROM continuumio/miniconda3

RUN apt-get update -y
RUN apt-get install -y curl build-essential gcc

ADD environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml
# Pull the environment name out of the environment.yml
RUN echo "source activate $(head -1 /tmp/environment.yml | cut -d' ' -f2)" > ~/.bashrc

RUN curl -sL https://deb.nodesource.com/setup_10.x | bash && apt-get install -y nodejs
WORKDIR /app
COPY package.json .
COPY webpack.config.js .
RUN npm install
COPY . /app
RUN npm run build
ENTRYPOINT ["/bin/bash", "entrypoint.sh"]
