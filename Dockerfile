# Используем образ Python
FROM python:3.9

# Устанавливаем зависимости для сборки C-расширений
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    libc-dev \
    build-essential \
    libffi-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы в контейнер
COPY . .

# Устанавливаем pip и зависимости
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Запускаем бота
CMD ["python", "bot.py"]
