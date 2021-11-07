FROM python:3.6

WORKDIR /forest_cover_Classification
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python", "main.py"]