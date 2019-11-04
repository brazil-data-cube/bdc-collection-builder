import os
from bdc_scripts import create_app


app = create_app(os.environ.get('ENVIRONMENT', 'ProductionConfig'))


if __name__ == '__main__':
    app.run()