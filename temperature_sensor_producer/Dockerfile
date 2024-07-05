# Wähle ein Basisimage
FROM python:3.9-slim

# Setze das Arbeitsverzeichnis im Container
WORKDIR /app

# Kopiere die requirements.txt Datei ins Arbeitsverzeichnis
COPY requirements.txt .

# Installiere die Python-Abhängigkeiten
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere den Rest des Anwendungscodes ins Arbeitsverzeichnis
COPY . .

# Setze das Standardkommando für den Container
CMD ["python", "producer.py"]
