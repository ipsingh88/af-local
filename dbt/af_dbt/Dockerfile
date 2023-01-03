FROM python:3.9-bullseye

COPY ./ /dbt/

WORKDIR /dbt

RUN pip install --upgrade pip && pip install dbt-postgres==1.2.0

RUN dbt deps 

ENTRYPOINT ["./entrypoint.sh"]
