FROM python

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY extract_games.py .
COPY transform_games.py .
COPY load_games.py .
COPY pipeline.py .

CMD python3 pipeline.py

