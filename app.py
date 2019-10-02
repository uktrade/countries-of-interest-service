import os
from flask import Flask

app = Flask(__name__)
cf_port = os.getenv("PORT")

@app.route('/')
def hello_world():
    return 'Hello, World!'

if __name__ == '__main__':
    if cf_port is None:
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        app.run(host='0.0.0.0', port=int(cf_port), debug=True)
        
