FROM python:3.13-slim
WORKDIR /app
COPY main.py /app/
COPY constants.py /app/
COPY db.py /app/
COPY jobs.py /app/
COPY models.py /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /data
CMD ["python", "main.py", "--mode", "stockPrice"]