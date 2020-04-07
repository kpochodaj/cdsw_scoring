# Model deployment options
This project demonstrates different options for serving machine learning models.

### Setup
Start a Python 3 Session with 1 CPU and 2 GB RAM.

### 1 Batch - Python with Impala
Example demonstrates how to:
- interact with data stored in Impala
- load serialized model
- use model to score data in dataframe
- save prediction into Impala

Pre-requisites:
- example table stored in Impala (telco_churn)
- ibis-framework (see below)

Open `code/batch_python_impala.py` in a workbench.

Execute from terminal:
!pip3 install --upgrade --force-reinstall  ibis-framework[impala]

Ignore the following error:
ERROR: jupyter-console 6.0.0 has requirement prompt-toolkit<2.1.0,>=2.0.0, but you'll have prompt-toolkit 1.0.15 which is incompatible.

Change IP for IMPALA HOST to match your private IP from provided spreadsheet:
IMPALA_HOST = os.getenv('IMPALA_HOST', '10.0.1.254')

Run code

### 2 Batch - Spark with CSV and Impala
Example demonstrates how to:
- interact with data stored in Impala
- load serialized model
- use model to score data in dataframe
- save prediction into Impala

Open `code/batch_spark.py` in a workbench.

Run code.


