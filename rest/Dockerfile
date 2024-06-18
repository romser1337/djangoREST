# syntax=docker/dockerfile:1

FROM python:3.10.6-slim-buster

WORKDIR /rest
RUN apt-get update
RUN apt-get install -y gdal-bin
RUN apt-get install -y libgdal-dev
RUN apt install -y g++
RUN pip install "setuptools<58.0.0"

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
RUN pip install gdal==2.4.0
RUN pip install geoserver-rest

COPY ./ .

#CMD ["gunicorn"  , "-b", "0.0.0.0:8000", "wsgi:application"]
CMD ["gunicorn", "--config", "gunicorn_config.py", "rest.wsgi:application"]

#CMD [ "python3", "manage.py", "runserver", "0.0.0.0:8080", "--noreload"]