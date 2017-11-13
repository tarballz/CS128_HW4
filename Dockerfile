FROM ubuntu:latest
MAINTAINER Ted & Payton
RUN ["apt-get", "update", "-y"]
RUN ["apt-get", "install", "-y", "python-pip", "python-dev"]
RUN ["pip", "install", "django"]
RUN ["pip", "install", "djangorestframework"]
RUN ["pip", "install", "requests"]
COPY ./hw3 /hw3
EXPOSE 8080
WORKDIR /hw3
RUN ["python", "manage.py", "migrate"]
CMD ["python", "manage.py", "runserver", "0.0.0.0:8080"]
