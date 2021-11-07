From continuumio/anaconda3:2020.11

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ADD . /forest_cover_Classification
WORKDIR /forest_cover_Classification

ENTRYPOINT ["python", "main.py"]