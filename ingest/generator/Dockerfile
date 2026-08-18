FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generator.py profile.py ./
ENV PROFILES_DIR=/profiles
ENTRYPOINT ["python", "-u", "generator.py"]
