FROM python as builder
COPY "requirements.txt" ./
RUN pip install -r requirements.txt

FROM builder as prod
COPY ./main.py /app
CMD ["python", "/app/main.py"]