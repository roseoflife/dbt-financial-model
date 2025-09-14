from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow(name="financial-reporting")
def dbt_financial_flow():
    # Run your dbt models in sequence
    dbt_operation = DbtCoreOperation(
        commands=[
            "dbt run --select stg_transactions",
            "dbt run --select int_net_revenues", 
            "dbt run --select mrt_customer_revenues"
        ],
        project_dir="dbt_project"
    )
    
    return dbt_operation.run()

if __name__ == "__main__":
    dbt_financial_flow()