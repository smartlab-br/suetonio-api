FROM smartlab/flask:latest
LABEL maintainer="smartlab-dev@mpt.mp.br"

COPY app /app/
COPY uwsgi.ini /etc/uwsgi/

RUN pip install -Iv kafka-python==1.4.7 redis==3.0.1

EXPOSE 5000
WORKDIR /app

ENTRYPOINT ["sh", "/start.sh"]
