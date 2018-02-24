from pymongo import MongoClient, UpdateOne
import os
import urllib
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import SstQueryDataOperator
from airflow.operators.python_operator import PythonOperator

def update_partner_statuses(*args, **kwargs):
    ti = kwargs['ti']
    flags= ti.xcom_pull(key=None, task_ids='query_update_partner_statuses')

    #Initiaslise Mongo db clients
    mongo_client_dev = MongoClient('mongodb://swift-np-admin:{0}@35.184.71.178:27017/'.format(urllib.quote('k2Ud)7vS?2Kg<u')))
    mongo_client_prod = MongoClient('mongodb://swift-p-admin:{0}@35.184.168.229:27017/'.format(urllib.quote('M7Oy@wqO43]708F')))

    #initialise Mongo db databases
    consumption_dev = mongo_client_dev['swift-metadata-dev']
    consumption_qa = mongo_client_dev['swift-metadata-qa']
    consumption_uat = mongo_client_prod['swift-metadata-uat']
    consumption_prod = mongo_client_prod['swift-metadata-prod']

    databases = [consumption_prod, consumption_uat, consumption_qa, consumption_dev]


    if len(flags) <= 0:
        return

    partners = {}

    # Iterated Through loaded data
    for flag in flags:
        master_account_code = flag['master_account_code']
        partner_report_date = flag['partner_report_date']
        load_datetime = flag['load_datetime']
        current_partner = partners.get(master_account_code)
        if current_partner:
            if partner_report_date > current_partner[0]:
                partners[master_account_code] = (partner_report_date, load_datetime)
        else:
            partners[master_account_code] = (partner_report_date, load_datetime)

    # Iterate Through Database List
    for db in databases:
        partner_status_collection = db.partner_status
        data = partner_status_collection.find({'_id': {'$in': [str(k) for k in partners.keys()]}})
        changed_partners = []

        # Check id some data is changed
        for partner_status in data:
            id = partner_status.get('_id')
            if not id:
                continue

            last_report_date_bq = partner_status.get('last_report_date_bq')
            load_datetime_bq = partner_status.get('load_datetime_bq')

            updated_tuple = partners.get(int(id))
            if not updated_tuple:
                continue

            last_report_date = None
            load_datetime = None

            if updated_tuple[0]:
                last_report_date = unicode(updated_tuple[0])
            if updated_tuple[1]:
                load_datetime = unicode(datetime.fromtimestamp(float(updated_tuple[1])).isoformat())

            if last_report_date != last_report_date_bq or load_datetime != load_datetime_bq:
                if last_report_date:
                    partner_status['last_report_date_bq'] = last_report_date
                if load_datetime:
                    partner_status['load_datetime_bq'] = load_datetime
                changed_partners.append(UpdateOne({'_id': id}, {'$set': partner_status}))

        # Do Bulk Update OPeration for all updated changes
        if len(changed_partners) > 0:
            result = db.partner_status.bulk_write(changed_partners)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['gevorg.khondkaryan@umusic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def createDag(dagName):
    return DAG(
        dag_id=dagName,
        schedule_interval=None,
        start_date=datetime(2017, 12, 17),
        default_args=default_args
    )

incDag = createDag('partner-status-update')

load_query = "SELECT master_account_code, partner_report_date, load_datetime FROM `umg-swift.consumption_speedlayer_staging.bt_flag` WHERE master_account_code IS NOT NULL"

query_update_partner_statuses_operator = SstQueryDataOperator(
        task_id='query_update_partner_statuses',
        sql=load_query,
        dag=incDag
    )

update_partner_statuses_operator = PythonOperator(
    task_id='update_partner_statuses',
    provide_context=True,
    python_callable=update_partner_statuses,
    dag=incDag)


query_update_partner_statuses_operator.set_downstream(update_partner_statuses_operator)