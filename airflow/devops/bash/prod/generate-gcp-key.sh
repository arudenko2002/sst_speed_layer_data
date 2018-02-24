#!/bin/bash
gcloud iam service-accounts list
gcloud iam service-accounts keys create umg-swift-key.json --iam-account=874919087886-compute@developer.gserviceaccount.com
export GOOGLE_APPLICATION_CREDENTIALS=umg-swift-key.json