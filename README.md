# Databricks Cluster Setup (AWS)

## Overview

This project documents the creation and configuration of an Apache Spark cluster using Databricks on AWS Marketplace.

The cluster is designed for distributed data processing using PySpark.

---

## Architecture

* Cloud Provider: AWS
* Platform: Databricks (via AWS Marketplace)
* Cluster Type: All-purpose compute
* Processing Engine: Apache Spark 4.0
* Language: Python (PySpark)

---

## Cluster Configuration

### General

* Cluster Name: cluster-spark-prod
* Policy: Unrestricted

### Runtime

* Databricks Runtime: 17.3 LTS
* Spark Version: 4.0.0
* Scala Version: 2.13
* Photon Acceleration: Enabled

### Infrastructure

* Driver Node (Master): Auto (Databricks managed)
* Worker Nodes: 2

#### Worker Configuration

* Instance Type: r5d.large
* Memory: 16 GB
* vCPUs: 2

---

## Scaling Configuration

* Autoscaling: Enabled
* Minimum Workers: 2
* Maximum Workers: 2

> Fixed cluster with 2 workers

---

## Cost Optimization

* Spot Instances: Enabled
* Auto Termination: 30 minutes

---

## Features Enabled

* Photon Engine (optimized execution)
* Distributed Processing (Spark cluster)
* Auto Scaling (fixed range)

---

## Notes

* Cluster created via AWS Marketplace integration
* Billing is handled through AWS account
* Instance types may be restricted due to trial limitations

---

## How to Start the Cluster

1. Go to **Compute**
2. Select cluster-spark-prod
3. Click **Start**
4. Wait until status is Running

---

## Next Steps

* Create Databricks Notebook
* Run PySpark jobs
* Build data pipeline
* Integrate with S3 / Delta Lake

---

## Author

Cleber Zumba de Souza
Data Engineer
