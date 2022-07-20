# syntax=docker/dockerfile:1

FROM python:3.10.4-alpine3.15
WORKDIR /server
COPY src/ ./src
RUN apk add --no-cache gcc musl-dev librdkafka-dev \
        && apk add build-base
COPY requirements.txt requirements.txt
RUN pip3 install -U setuptools pip \
        && pip3 install --no-cache-dir -r requirements.txt
COPY config.ini server.py ./
EXPOSE 30205
ENTRYPOINT ["python3"]
CMD ["server.py"]