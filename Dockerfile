FROM acidrain/multi-python

RUN apt-get update && apt-get install -y postgresql libpq-dev
RUN pip install nox

CMD ["tail", "-f", "/dev/null"]
