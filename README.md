# IDS 568 MLOps - Milestone 4: Distributed & Streaming Pipeline
[![Build Status](https://github.com/kiranmusu14/ids568-milestone4-kiranmusu14/actions/workflows/python-app.yml/badge.svg)](https://github.com/kiranmusu14/ids568-milestone4-kiranmusu14/actions/workflows/python-app.yml)

## Project Overview
This repository contains a distributed feature engineering pipeline built with PySpark, alongside an optional streaming ingestion pipeline. [cite_start]The goal of this project is to evaluate the scaling behavior, throughput optimization, and architectural trade-offs between single-machine (local) execution and multi-worker (distributed) execution on datasets exceeding 10 million rows [cite: 198-203].

## [cite_start]Reproducibility & Setup Instructions [cite: 118-122]
[cite_start]Any user can clone this repository and reproduce the exact performance results in under 10 minutes[cite: 121]. 

### Prerequisites
* Python 3.9+ 
* Apache Spark / Java Runtime Environment (JVM 17 recommended)

## Installation
1. Clone the repository:
   ```bash
   git clone [https://github.com/kiranmusu14/ids568-milestone4-kiranmusu14.git](https://github.com/kiranmusu14/ids568-milestone4-kiranmusu14.git)
   cd ids568-milestone4-kiranmusu14
