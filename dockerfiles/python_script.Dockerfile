#Deriving the latest base image
FROM python:3.9

#Labels as key value pair
LABEL Author="yuanqingyeoh"

RUN pip install confluent-kafka websockets
# Any working directory can be chosen as per choice like '/' or '/home' etc
# i have chosen /usr/app/src
WORKDIR /usr/app/src

CMD [ "python", "./socket_producer.py"]