# Databricks CI/CD Orders Pipeline

A PySpark ETL project demonstrating CI/CD with GitHub Actions
and Databricks Asset Bundles.

## Architecture

1. Developer pushes code to GitHub.
2. GitHub Actions installs dependencies.
3. PySpark unit tests execute.
4. Databricks Asset Bundles deploys the Python source files.
5. Databricks Job executes the pipeline.

## Project Structure

- src/: Python application
- tests/: PySpark unit tests
- resources/: Databricks Job configuration
- databricks.yml: Bundle configuration
- requirements.txt: Testing dependencies
- .github/workflows/ci.yml: CI/CD workflow

## Local Testing

Install dependencies:

    pip install -r requirements.txt

Run tests:

    python -m pytest tests/ -v

## Databricks Deployment

Authenticate:

    databricks auth login --host https://dbc-a0541325-6964.cloud.databricks.com

Validate:

    databricks bundle validate -t dev

Deploy:

    databricks bundle deploy -t dev

Run the Job:

    databricks bundle run orders_pipeline -t dev

## GitHub Secrets

Configure these repository secrets:

- DATABRICKS_HOST
- DATABRICKS_TOKEN

Deployment to Databricks Dev happens on pushes to the main branch.

## Business Logic

- Remove invalid orders
- Convert order_date to DateType
- Convert amount to DoubleType
- Calculate daily revenue
