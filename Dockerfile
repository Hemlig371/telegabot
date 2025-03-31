FROM python:3.9-slim

# Установка зависимостей для сборки
RUN apt-get update && apt-get install -y build-essential python3-dev

# Установка зависимостей проекта
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Копирование исходных кодов
COPY . /app

WORKDIR /app
CMD ["python", "bot.py"]
