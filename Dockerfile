FROM thekevjames/nox

COPY docker_runner /root/.ssh/id_rsa
COPY tests/ssh-config /root/ssh/config

RUN touch /root/.ssh/known_hosts && ssh-keyscan tessgit.mit.edu >> /root/.ssh/known_hosts

RUN apt-get update && apt-get install -y postgresql libpq-dev
