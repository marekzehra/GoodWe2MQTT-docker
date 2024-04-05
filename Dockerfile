FROM python:3

WORKDIR /usr/src/app

COPY GoodWe2MQTT .

RUN pip3 install --no-cache-dir -r requirements.txt

WORKDIR /goodwe2mqtt

CMD [ "python3", "/usr/src/app/goodwe2mqtt.py" ]