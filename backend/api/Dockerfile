FROM --platform=linux/amd64 python:3.12
RUN apt-get update && apt-get install -y ca-certificates && apt-get clean
RUN pip install --upgrade pip
WORKDIR /srv
COPY requirements.txt .
COPY backend/api/requirements.txt requirements-server.txt
RUN pip install -r requirements-server.txt -r requirements.txt
COPY botnim/ ./botnim
COPY setup.py ./
COPY backend/api/server.py .
COPY backend/api/api_server.sh .
RUN pip install -e .
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["./api_server.sh"]
