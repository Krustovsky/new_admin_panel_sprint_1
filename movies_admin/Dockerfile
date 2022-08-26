# Укажите необходимую версию python
FROM python:3.10

# Выберите папку, в которой будут размещаться файлы проекта внутри контейнера 
WORKDIR /opt/app

ENV PYTHONUNBUFFERED=1

# Скопируйте в контейнер файлы, которые редко меняются
COPY requirements.txt requirements.txt

# Установите зависимости
RUN  pip install --upgrade pip \
     && pip install -r requirements.txt
# Скопируйте всё оставшееся. Для ускорения сборки образа эту команду стоит разместить ближе к концу файла. 
COPY . .

# Укажите порт, на котором приложение будет доступно внутри Docker-сети
EXPOSE 8000

# Укажите, как запускать ваш сервис
CMD ["python","manage.py","runserver"] 
