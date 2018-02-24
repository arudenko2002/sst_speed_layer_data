#!/bin/bash

#remove existing dags and plugins
sudo rm -rf /opt/airflow/dags/consumption/*
#sudo rm -Rf /opt/airflow/plugins/*

sudo cp -R dags /opt/airflow/dags/consumption
#sudo cp -Rf sst-speed-layer-data/airflow/plugins/* /opt/airflow/plugins