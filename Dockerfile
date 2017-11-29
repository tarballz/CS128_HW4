FROM ubuntu:latest
MAINTAINER Ted, Payton & Alex
RUN ["apt-get", "update", "-y"]
RUN ["apt-get", "install", "-y", "python3-pip", "python3-dev"]
RUN ["pip3", "install", "django"]
RUN ["pip3", "install", "djangorestframework"]
RUN ["pip3", "install", "requests"]
COPY ./hw3 /hw3
EXPOSE 8080
WORKDIR /hw3
RUN ["python3", "manage.py", "makemigrations"]
RUN ["python3", "manage.py", "migrate"]
CMD ["python3", "manage.py", "runserver", "0.0.0.0:8080"]
