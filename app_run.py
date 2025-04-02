from flask import Flask
import os
from src.routes import routes_blueprint  # Certifique-se de que 'routes.py' está na mesma pasta

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

# Registrar o Blueprint corretamente
app.register_blueprint(routes_blueprint)

if __name__ == '__main__':
    print()
    app.run(host='0.0.0.0', port=port)
