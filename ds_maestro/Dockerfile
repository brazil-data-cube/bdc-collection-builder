FROM python:3-alpine
MAINTAINER Ricardo Cartaxo <ricardo.cartaxo@inpe.br> 

# Install dependencies
RUN apk update
RUN apk add --no-cache \
            --allow-untrusted \
            gcc musl-dev mariadb-dev \
            --repository \
            http://dl-3.alpinelinux.org/alpine/edge/testing
			
# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD ./requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.python.org -r requirements.txt
# Setting environment variables
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH=$PYTHONPATH:/ds_include

# Expose the Flask port
EXPOSE 5020

# Run the maestro application
CMD [ "python3", "maestro_wsgi.py" ]
