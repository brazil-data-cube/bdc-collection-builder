FROM  registry.dpi.inpe.br/espa-science 
MAINTAINER Ricardo Cartaxo <ricardo.cartaxo@inpe.br> 



RUN yum -y install epel-release
RUN yum -y install python34 \
	python34-devel \
	python34-setuptools


RUN cd /usr/lib/python3.4/site-packages/ && python3.4 easy_install.py pip
# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD ./requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip3 install -r requirements.txt

# Setting environment variables
ENV PYTHONUNBUFFERED 1

ENV PATH $PATH:/usr/local/espa-surface-reflectance/lasrc/bin/:/app

# Expose the Flask port
EXPOSE 5032


# Run the flssk application
CMD [ "python3.4", "rc_espa.py" ]
