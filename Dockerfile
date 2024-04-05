FROM python:3

WORKDIR /goodwe2mqtt

COPY GoodWe2MQTT /usr/src/app

RUN pip3 install --no-cache-dir -r /usr/src/app/requirements.txt

CMD [ "python3", "/usr/src/app/goodwe2mqtt.py" ]