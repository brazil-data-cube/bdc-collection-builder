from cubesearch import app
import os

if __name__ == '__main__':
    #app.run(debug=True,host=os.environ.get('BASE_HOST'),port=os.environ.get('BASE_PORT'))
    app.run(debug=True,host='0.0.0.0',port=os.environ.get('BASE_PORT'))
