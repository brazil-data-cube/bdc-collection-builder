FROM geographica/gdal2
RUN apt-get update -y && apt-get install -y \
	python-pip \
	python-dev \
	curl \
	unzip \
    wget && \
    rm -rf /var/lib/apt/lists/* 

# Upgrade pip
RUN pip install --upgrade pip

# Set the working directory to /app
WORKDIR /app

# Install any needed packages specified in requirements.txt
ADD requirements.txt /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Install Sen2Cor-02.05.05-Linux64.run
ADD Sen2Cor-02.05.05-Linux64.run /app 
RUN chmod a+x /app/Sen2Cor-02.05.05-Linux64.run && \
  bash /app/Sen2Cor-02.05.05-Linux64.run --target /home && \
  rm Sen2Cor-02.05.05-Linux64.run
ENV PATH $PATH:/home/bin/
#ADD ./CCI4SEN2COR /home/lib/python2.7/site-packages/sen2cor/aux_data

# Setting environment variables
ENV PYTHONUNBUFFERED 1

# Expose the Flask port
EXPOSE 5031

# Run the sen2cor application
CMD [ "python", "rc_sen2cor.py" ]
