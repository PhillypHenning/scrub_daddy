FROM python:latest

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY scrub_daddy.py scrub_daddy.py

CMD ["python", "scrub_daddy.py"]