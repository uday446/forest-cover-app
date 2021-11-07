From continuumio/anaconda3:2020.11

COPY requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

COPY . /forest_cover_Classification
WORKDIR /forest_cover_Classification

ENTRYPOINT ["python", "main.py"]