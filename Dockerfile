FROM apache/airflow:3.0.3
COPY requirements.txt .

USER root

RUN apt-get update && apt-get install -y git locales

RUN sed -i -e 's/# pt_BR.UTF-8 UTF-8/pt_BR.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen

USER airflow

ARG GIT_USERNAME
ARG GIT_TOKEN

RUN sed -i "s/\${GIT_USERNAME}/${GIT_USERNAME}/g" requirements.txt && \
    sed -i "s/\${GIT_TOKEN}/${GIT_TOKEN}/g" requirements.txt

RUN git config --global credential.helper store && \
    echo "https://${GIT_USERNAME}:${GIT_TOKEN}@github.com" > ~/.git-credentials

RUN pip install --no-cache-dir -r requirements.txt

RUN PYTHONUSERBASE=/home/airflow/.local pip install --no-cache-dir --upgrade --force-reinstall git+https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/raizen-energy/raizen-power-trading-libs-middle.git
RUN PYTHONUSERBASE=/home/airflow/.local pip install --no-cache-dir --upgrade --force-reinstall git+https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/raizen-energy/trading-middle-newave.git



RUN rm -f ~/.git-credentials && \
    git config --global --unset credential.helper

COPY config/airflow.cfg /opt/airflow/config/airflow.cfg
COPY .env /home/airflow/.env

WORKDIR /opt/airflow

EXPOSE 8080
