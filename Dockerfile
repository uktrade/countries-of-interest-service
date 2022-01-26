FROM python:3.7

RUN apt-get update -y

ADD requirements.txt /tmp/requirements.txt

ADD scripts scripts
RUN scripts/install_dockerize.sh
RUN scripts/install_node.sh
RUN scripts/install_python_packages.sh

WORKDIR /app

COPY . /app

RUN /app/scripts/compile_assets.sh
CMD /app/scripts/entrypoint.sh
