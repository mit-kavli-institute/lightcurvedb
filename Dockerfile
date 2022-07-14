FROM python:3.9-alpine
WORKDIR /testing
COPY requirements.txt /testing
COPY testrequirements.txt /testing

RUN \
    apk add --no-cache postgresql-libs && \
    apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
    apk add hdf5-dev gfortran build-base wget freetype-dev libpng-dev openblas-dev && \
    pip install -r requirements.txt && pip install -r testrequirements.txt && \
    apk --purge del .build-deps
COPY . /testing
CMD ["ptw"]
