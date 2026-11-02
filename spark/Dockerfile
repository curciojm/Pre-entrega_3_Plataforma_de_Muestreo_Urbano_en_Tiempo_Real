FROM apache/spark:4.1.4-python3

USER root
WORKDIR /app

COPY streaming_processor.py .

RUN chown -R 185:185 /app

USER 185

CMD ["python", "/app/streaming_processor.py"]