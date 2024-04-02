FROM python:3.10

WORKDIR /opt/prefect/flows

COPY requirements.txt /tmp/
RUN python -m pip install -r /tmp/requirements.txt