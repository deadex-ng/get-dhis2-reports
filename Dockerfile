FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run main.py first, then resolve_names.py
CMD ["bash", "-c", "python main.py && python resolve_names.py"]
