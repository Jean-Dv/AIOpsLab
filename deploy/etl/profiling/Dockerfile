FROM python:3.12.8-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY profile.py ./
ENTRYPOINT ["python", "/app/profile.py"]
