#!/bin/bash

export PATH=$PATH:/usr/lib/uwsgi/plugins/
export FLASK_DEBUG=0
export FLASK_APP=/app/main.py

if [ -e $FLASK_APP ] ; then

	if [ "$1" = 'debug' ] ; then
		echo DEBUG mode
        	export FLASK_DEBUG=1
	fi

	if [ "$1" = 'test' ] ; then
		echo TEST mode
		cd /app
		find ./ -type f -name '*.py' | xargs pylint
		nosetests --traverse-namespace --with-xunit --xunit-file=/tmp/nosetests.xml --exe --py3where .
	fi

	if [ "$1" = 'uwsgi' ] ; then
		echo uWSGI mode
        	exec uwsgi --ini /etc/uwsgi/conf.d/uwsgi.ini --die-on-term
	fi

	if [ "$1" = 'terminal' ] ; then
		echo TERMINAL mode
        	exec /bin/sh
	fi

	exec flask run --host=0.0.0.0 --port=5000
else
	echo -e "\n Erro: File /app/main.py not found!"
fi

echo -e "\n ==> Read how to use this in https://github.com/smartlab-flpo-br/flask"