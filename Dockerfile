
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

WORKDIR /app
ADD ./requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY ./test /test
COPY ./app /app
ENV PYTHONUNBUFFERED=TRUE
CMD ["uvicorn", "main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]


