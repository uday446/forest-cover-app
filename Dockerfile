From continuumio/anaconda3:2020.11

ADD . /forest_cover_Classification
WORKDIR /forest_cover_Classification

ENTRYPOINT ["python", "main.py"]