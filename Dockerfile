FROM python:3.9-alpine

WORKDIR /testing
ENV MAKEFLAGS="-j10"

RUN \
    apk update && \
    apk add --no-cache git openssh postgresql-libs && \
    apk add --no-cache --virtual .build-deps gcc musl-dev libpq-dev postgresql-dev && \
    apk add hdf5-dev gfortran build-base wget freetype-dev libpng-dev openblas-dev && \
    pip install --upgrade pip && \
    pip install --upgrade setuptools && \
    pip install psycopg2 tox && \
    apk --purge del .build-deps

RUN mkdir -p /root/.config/tic && touch /root/.config/tic/db.conf


CMD ["ptw"]
