FROM python:3

WORKDIR /usr/src/app

COPY GoodWe2MQTT .

RUN pip3 install --no-cache-dir -r requirements.txt
### create log and config dir
RUN mkdir -p /goodwe2mqtt

# CMD [ "python3", "./godwe2mqtt.py" ]