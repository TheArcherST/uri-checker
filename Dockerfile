FROM python:3.12

WORKDIR /src/usr/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
