FROM python:3.9-slim-buster


WORKDIR /app

COPY requirements.txt requirements.txt

COPY . .

RUN pip3 install -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/"

CMD ["python3", "src/main.py"]
