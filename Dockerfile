FROM python:3.7-slim-buster

WORKDIR /workspace

RUN useradd -s /bin/bash user

RUN pip3 install --upgrade pip

RUN pip3 install luigi

RUN pip3 install praw

RUN pip3 install schedule

RUN pip3 install openpyxl

RUN pip3 install pandas
