# IDS 568 MLOps - Milestone 4: Distributed & Streaming Pipeline
[![Python application](https://github.com/kiranmusu14/ids568-mlops-project-mil4/actions/workflows/python-app.yml/badge.svg)](https://github.com/kiranmusu14/ids568-mlops-project-mil4/actions/workflows/python-app.yml)

## Project Overview
This repository contains a distributed feature engineering pipeline built with PySpark, alongside an optional streaming ingestion pipeline. The goal of this project is to evaluate the scaling behavior, throughput optimization, and architectural trade-offs between single-machine (local) execution and multi-worker (distributed) execution on datasets exceeding 10 million rows.

## Reproducibility & Setup Instructions
Any user can clone this repository and reproduce the exact performance results in under 10 minutes.

### Prerequisites
* Python 3.9+
* Apache Spark / Java Runtime Environment (JVM 17 recommended)

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/kiranmusu14/ids568-mlops-project-mil4.git
   cd ids568-mlops-project-mil4
