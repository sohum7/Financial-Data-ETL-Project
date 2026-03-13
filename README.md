# Financial-Data-ETL-Project
## Table of Contents
#TODO

## Overview
Google Cloud Platform (GCP) is the cloud service provider (csp) utilized for this project.
Services used: 
## Architecture
Google Cloud Platform (GCP) is the cloud service provider (csp) utilized for this project.
Cloud Run will be used for running the docker based ETL pipeline

## RESPONSILBITIES

### extraction
extractor.py
    input(s):
        symbols, batch date, start date, end date
    output(s):
        gcs file path to raw json
    purpose:
        submits api request
        returns the data as a json
        file name: {data category}_{start date}_{end date}_{hash of symbols}.json

### transformation
transformer.py
    input(s):
        gcs file path to raw json
    output(s):
        df
    purpose:
        remove non-data and metadata fields
        transforms the data
            date fields and formatting
            null/missing data
            reordering fields

### loading part 1
loader.py
    input(s):
        df
    output(s):
        staging table with data from DataFrame
    purpose:
        loads data into staging table

### loading part 2
merger.py
    input(s):
        staging table
    insert into:
        main table
    purpose:
        merges the staging table into the main table
        drop staging table? or via retention policy?

Cloud Scheduler
    run the etl with the docker image