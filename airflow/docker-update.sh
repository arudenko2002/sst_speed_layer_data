#!/bin/bash

#remove existing dags and plugins
sudo rm -rf /opt/airflow/dags/consumption/*
#sudo rm -rf /opt/airflow/plugins/*

sudo cp -rf dags/* /opt/airflow/dags/consumption
#sudo cp -rf sst-speed-layer-data/airflow/plugins/* /opt/airflow/plugins