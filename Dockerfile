# syntax=docker/dockerfile:1

FROM python:3.10.4-alpine3.15
WORKDIR /server
COPY src/ ./src
RUN apk add --no-cache gcc musl-dev librdkafka-dev
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
COPY config.ini server.py ./
ENTRYPOINT ["python3"]
CMD ["server.py"]