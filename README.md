# ETL-with-airflow
Situation: I was tasked with creating an end-to-end data pipeline for a movie analytics company. The goal was to build a fact table that brings together data from multiple sources including user purchases, movie reviews, and review session logs. 

Task: My role was to implement the ETL pipeline and data warehouse using Airflow, Spark, and cloud resources. Key tasks included:

- Setting up Airflow in GCP to orchestrate the pipeline

- Creating connections between Airflow and the postgres database 

- Building Spark jobs to transform the CSV data

- Structuring the dimensional model in PostgresSQL

- Loading transformed data into dimension and fact tables

Action: To implement the pipeline, I first set up a GCP Cloud Composer instance for Airflow and connected it to a Cloud SQL Postgres database. I configured Cloud Storage buckets for staging and created Airflow DAGs to automate ETL jobs with Spark on Dataproc. The Spark jobs cleaned and joined the CSV files into analytics-ready Parquet files that I then loaded into the PostgresSQL data warehouse structured per the star schema. I also added data quality checks in the DAGs.

Result: The implemented solution automated the ETL process from end to end. Key outputs included:

- Automated workflow DAGs in Cloud Composer
- Scalable Spark jobs for transforming large amounts of data 
- Load times reduced from 8 hours to 40 minutes
- Data warehouse structured for easy BI analytics
- Data quality checks to ensure accuracy

The pipeline provided significant improvements in automation, performance, and analytics-readiness compared to the previous manual process. My technical knowledge of Airflow, Spark, and cloud infrastructure was crucial to implementing the optimal architecture.
