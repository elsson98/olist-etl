import logging
from etl_processing import run_etl, setup_logging
from load_data import load_to_sql_server


# Run scripts
def main():
    logger = setup_logging()
    logger.info("Starting ETL process")

    processed_dfs = run_etl(logger)

    connection_string = "mssql+pyodbc://sa:Admin123!@localhost/olist_dw?driver=ODBC+Driver+17+for+SQL+Server"
    logger.info("Starting data loading to SQL Server")

    load_to_sql_server(processed_dfs, connection_string, logger)


if __name__ == "__main__":
    main()