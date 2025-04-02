from flask import Flask
import os
from routes import routes_blueprint

app = Flask(__name__)
port = int(os.environ.get('PORT', 5001))


app.register_blueprint(routes_blueprint)

if __name__ == '__main__':

    app.run(host='0.0.0.0', port=port)
