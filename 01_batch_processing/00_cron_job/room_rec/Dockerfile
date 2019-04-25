FROM python:3.6-slim
LABEL maintainer Garrett Hoffman <garrett@stocktwits.com>

RUN mkdir -p /src/
WORKDIR /src

COPY requirements.txt /src

RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt 

COPY room_rec.py /src/

CMD [ "python", "room_rec.py" ]