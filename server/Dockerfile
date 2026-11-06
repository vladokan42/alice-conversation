FROM python:3.13-slim-bookworm
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
    CONVERSATION_MODEL_PATH=/models/vosk-model-small-ru-0.22
COPY requirements.txt .
# All native packages are wheels; srt is pure Python and has no published wheel.
RUN pip install --no-cache-dir --prefer-binary \
    --only-binary=aiohttp,vosk,webrtcvad-wheels -r requirements.txt
COPY server.py .
USER 1000:1000
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).read()"]
CMD ["python", "server.py"]
