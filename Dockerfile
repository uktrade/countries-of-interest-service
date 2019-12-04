FROM nikolaik/python-nodejs
WORKDIR /app
COPY package.json .
COPY webpack.config.js .
RUN npm install
COPY static static
RUN npm run build
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /app
ENTRYPOINT ["/bin/bash", "entrypoint.sh"]
