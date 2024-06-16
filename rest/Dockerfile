# syntax=docker/dockerfile:1

FROM python:3.10.6-slim-buster

WORKDIR /rest
RUN apt-get update && \
    apt-get install -y binutils libproj-dev gdal-bin ffmpeg libgdal-dev
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY .. .

CMD [ "python3", "manage.py", "runserver", "0.0.0.0:8000", "--noreload"]