FROM python:3.6-slim
LABEL maintainer Garrett Hoffman <garrett@stocktwits.com>

RUN mkdir -p /src/
WORKDIR /src

COPY requirements.txt /src

RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt 

COPY minutely_sym_sent.py /src/

CMD [ "python", "minutely_sym_sent.py" ]