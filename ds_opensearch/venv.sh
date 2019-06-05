#!/bin/bash

set -e  # If occur any error, exit

function to_console {
    echo -e "\n*** $1 ***\n"
}

sudo apt-get install  python3-pip python3-dev virtualenv libmysqlclient-dev

to_console "creating virtual env on venv folder"
virtualenv -p python3 venv

to_console "Activating virtualenv"
source venv/bin/activate

to_console "Checking up dependencies"
if [ ! -z "$1" ]
    then
        to_console "Running with proxy "$1
        pip install -r requirements.txt --proxy=$1
    else
        to_console 'Runing with no proxy'
        pip install -r requirements.txt
fi



