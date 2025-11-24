FROM python:3.11

WORKDIR /app

COPY tests/requirements.txt .
RUN pip install -r requirements.txt


CMD ["/bin/bash", "-c",  "pytest -m integration -v" ]