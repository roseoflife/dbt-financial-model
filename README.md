# README.md
# dbt Financial Model Project

This repository contains a dbt project for transforming financial data using a layered approach: staging, intermediate, and mart layers. It integrates with dbt Cloud and Snowflake to produce a customer revenue mart table for analytics.

## Project Structure
- `models/staging/`: Lightly transformed raw data (e.g., `stg_transactions.sql`)
- `models/intermediate/`: Complex transformations (e.g., `int_net_revenue.sql`)
- `models/marts/`: Business-ready tables (e.g., `mrt_customer_revenue.sql`)
- `dbt_project.yml`: dbt configuration file
- `slides/presentation.md`: Slide deck outline for project presentation

## Setup Instructions
1. Clone this repository: `git clone <repository-url>`
2. Install dbt: `pip install dbt-snowflake`
3. Configure your `profiles.yml` with Snowflake credentials (not included in repo)
4. Run `dbt deps` to install dependencies
5. Run `dbt run` to execute models
6. View results in Snowflake (customer revenue mart table)

## Tools
- dbt Cloud
- Snowflake
# README.md end
