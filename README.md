# 4M

Metadata Mining of Mobility Data for MODI

## Contents of the Repository

This repository is structured into the following method-based folders:

- **01_ETL**: Extract, Transform, Load steps for collecting and preparing metadata.
- **02_Categorization**: Classification of mobility data and thematic clustering.
- **03_NER**: Named Entity Recognition — training data and models.
- **04_QC**: Quality control of the metadata.
- **05_Language_Detection**: Evaluation and testing of language detection methods.
- **06_Final_Workflow**: Final integrated workflow combining all previous components.

## Setting up the prerequisites for the PostgreSQL metadata database

To configure the database connection, add the following content to a file named `db_config.json` inside the folder `06_Final_Workflow\resources`:

```json
{
  "host": "hostname",
  "port": 5432,
  "user": "bth_4m",
  "password": "bth_4m"
}
```

## Installing Required Python Packages

To run this project, you need to install all required Python packages listed in the `requirements.txt` file.

### Step 1: (Optional) Create and activate a virtual environment

Creating a virtual environment is recommended to avoid dependency conflicts.

#### On Windows:

```bash
python -m venv 4M_env
4M_env\Scripts\activate
```

### Step 2: Install dependencies

Install all required packages:

```bash
pip install -r requirements.txt
```
