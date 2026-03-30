# Databricks Cluster Setup (AWS)

## Overview

This project documents the creation and configuration of an Apache Spark cluster using Databricks on AWS.

The cluster is designed for distributed data processing using PySpark in a production-like environment.

---

## Architecture

- Cloud Provider: AWS
- Platform: Databricks
- Cluster Type: All-purpose compute
- Processing Engine: Apache Spark 3.4.1
- Language: Python (PySpark)

---

## Cluster Configuration

### General

- Cluster Name: cluster-spark
- Policy: Unrestricted

### Runtime

- Databricks Runtime: 13.3 LTS
- Spark Version: 3.4.1
- Scala Version: 2.12
- Photon Acceleration: Enabled

### Infrastructure

- Driver Node: Databricks managed
- Worker Nodes: 2

#### Worker Configuration

- Instance Type: m5d.xlarge
- Memory: 16 GB
- vCPUs: 4

---

## Scaling Configuration

- Autoscaling: Enabled
- Minimum Workers: 2
- Maximum Workers: 2

> Fixed-size distributed cluster (2 workers)

---

## Cost Optimization

- Auto Termination: 30 minutes

---

## Features Enabled

- Photon Engine (optimized execution)
- Distributed processing using Spark
- Cluster autoscaling (fixed range)

---

## Notes

- Workspace created using Databricks Account Console
- AWS IAM roles and S3 storage configured automatically
- Billing is handled through AWS account

---


## Author

Cleber Zumba de Souza  
Data Engineer
