FROM ubuntu:latest
MAINTAINER Ted, Payton & Alex
RUN ["apt-get", "update", "-y"]
RUN ["apt-get", "install", "-y", "python3-pip", "python3-dev"]
RUN ["pip3", "install", "django"]
RUN ["pip3", "install", "djangorestframework"]
RUN ["pip3", "install", "requests"]
COPY ./hw4 /hw4
EXPOSE 8080
WORKDIR /hw4
RUN ["python3", "manage.py", "makemigrations"]
RUN ["python3", "manage.py", "migrate"]
RUN ["python3", "manage.py", "flush", "--no-input"]
CMD ["python3", "manage.py", "runserver", "0.0.0.0:8080"]
