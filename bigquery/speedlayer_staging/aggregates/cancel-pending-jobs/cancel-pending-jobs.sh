#!/bin/bash


while true;
do
    # write list of all pending job id's into a text file
    bq ls -aj -n 15000  umg-swift | grep PENDING | awk '{print $1}' > /tmp/pending_jobs.txt

    # check if file is not empty, otherwise exit because there are no more pending jobs
    if [ -s /tmp/pending_jobs.txt ]
    then
        #loop through job ids and cancel them one by one
        while read job_id;
        do
            bq --nosync cancel $job_id
            echo "Job $job_id cancelled"
        done < /tmp/pending_jobs.txt

        #remove temp file with cancelled jobs
        rm /tmp/pending_jobs.txt
    else
        echo "All pending jobs are cancelled"
        exit 0
    fi

done