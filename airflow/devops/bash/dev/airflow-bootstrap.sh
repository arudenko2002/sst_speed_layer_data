#!/bin/bash
CLOUD_SQL_INSTANCE=umg-dev:us-central1:umg-swift-cloud-sql
EXECUTOR=LocalExecutor
SQL_DB_USER=root
SQL_DB_PASSWORD=pass@word1

# upgrade airflow
# - uninstall previous version
sudo pip uninstall airflow -y

# - install most recent version
sudo pip install --upgrade apache-airflow

# upgrade Google Cloud SQL Proxy
sudo systemctl stop google-cloud-sql-proxy

cd /home/airflow
sudo rm cloud_sql_proxy
wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
sudo chmod +x cloud_sql_proxy

sudo systemctl start google-cloud-sql-proxy

# change SQL cloud instance in airflow config file
sudo sed -i "/GOOGLE_CLOUD_SQL_INSTANCE/ c\GOOGLE_CLOUD_SQL_INSTANCE=$CLOUD_SQL_INSTANCE=tcp:3306" /etc/sysconfig/airflow

# restart cloud sql proxy service
sudo systemctl restart google-cloud-sql-proxy

# change executor to LocalExecutor
sudo sed -i "/executor = SequentialExecutor/ c\executor = $EXECUTOR" /home/airflow/airflow/airflow.cfg

# change database to Cloud SQL instance
sudo sed -i "/sql_alchemy_conn/ c\sql_alchemy_conn = mysql+mysqldb://$SQL_DB_USER:$SQL_DB_PASSWORD@127.0.0.1/airflow" /home/airflow/airflow/airflow.cfg

# disable sample DAGs
sudo sed -i "/load_examples/ c\load_examples = False" /home/airflow/airflow/airflow.cfg

# add fernet key
sudo sed -i "/fernet_key/ c\fernet_key = y6yEMlmG4TJryjgFXj1_FZYkApC2U7kdNqiaoV3NsZY=" /home/airflow/airflow/airflow.cfg

# initialize AirFlow metadata database
sudo airflow initdb

# restart AirFlow webserver
sudo systemctl restart airflow-webserver

# restart AirFlow scheduler
sudo systemctl restart airflow-scheduler

