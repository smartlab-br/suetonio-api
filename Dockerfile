FROM smartlab/flask-dataviz:latest
LABEL maintainer="smartlab-dev@mpt.mp.br"

USER root

COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

USER uwsgi

COPY app /app/
COPY uwsgi.ini /etc/uwsgi/

ENTRYPOINT ["sh", "/start.sh"]
