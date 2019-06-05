FROM python:3-alpine
MAINTAINER Jeferson Arcanjo <jeferson.arcanjo@funcate.org.br> 

# Install dependencies
RUN apk update
RUN apk add --no-cache gcc musl-dev mariadb-dev

# Prepare work directory
RUN mkdir -p /app
WORKDIR /app

# Get opensearch source and install python requirements
COPY requirements.txt /app
RUN pip install -r requirements.txt

# Setting environment variables
ENV PYTHONUNBUFFERED 1

# Expose the Flask port
EXPOSE 5003

# Run the opensearch application
CMD [ "python3", "wsgi.py" ]
