# Используем официальный образ Python
FROM python:3.9-slim

# Устанавливаем зависимости для сборки aiohttp
RUN apt-get update && apt-get install -y gcc libpq-dev python3-dev

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы в контейнер
COPY . .

# Обновляем pip и устанавливаем зависимости
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Запускаем бота
CMD ["python", "bot.py"]
