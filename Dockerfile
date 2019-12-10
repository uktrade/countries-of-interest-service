FROM continuumio/miniconda3
ADD environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml
# Pull the environment name out of the environment.yml
RUN echo "source activate $(head -1 /tmp/environment.yml | cut -d' ' -f2)" > ~/.bashrc
ENV PATH /opt/conda/envs/$(head -1 /tmp/environment.yml | cut -d' ' -f2)/bin:$PATH

RUN apt-get update -y
RUN apt-get install -y curl build-essential
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash && apt-get install -y nodejs
WORKDIR /app
COPY package.json .
COPY webpack.config.js .
RUN npm install
COPY . /app
RUN npm run build
ENTRYPOINT ["/bin/bash", "entrypoint.sh"]
