FROM python:3-alpine
MAINTAINER Jeferson Arcanjo <jeferson.arcanjo@funcate.org.br> 

RUN apk update
RUN apk add --no-cache gcc musl-dev mariadb-dev

# Prepare work directory
RUN mkdir -p /app
WORKDIR /app

# Get source and install python requirements
COPY requirements.txt /app
RUN pip install -r requirements.txt

RUN apk del g++ mariadb-dev musl-dev&& \
apk add --no-cache mariadb-connector-c

# Expose the Flask port
EXPOSE 5000

# Run the opensearch application
CMD [ "gunicorn", "-w 2", "-b :5000", "wsgi:app", "--reload" ]
#CMD ["python3 wsgi.py"]
