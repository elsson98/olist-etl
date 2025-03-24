# Understanding `etl_processing.py`

## What the Script Does

The `etl_processing.py` script processes data from the Olist dataset. It reads CSV files, cleans and transforms the data, calculates business metrics, and saves the results as new clean files.

## Key Functions in Detail

### `run_etl()`

- **Orchestration function** that controls the entire ETL workflow.
- Processes datasets in a specific order to handle dependencies:
  - First processes base datasets (customers, geolocation, payments, etc.).
  - Then processes `order_items`, which depends on orders and payments.
  - Finally, creates window functions which depend on multiple processed datasets.
- **Output management**:
  - Saves each processed dataset with a `clean_` prefix.
- **Returns**:
  - Dictionary of all processed DataFrames for downstream use in data loading.

### `process_order_items(df, orders_df=None, payments_df=None)`
- **Validations**:
  - Ensures data conforms to expected schema using `pandera`.
- **Calculated fields**:
  - `total_price`: Adds product price and freight value.
  - `profit_margin`: Subtracts freight value from price.
  - `delivery_time_days`: Calculates days between purchase and delivery.
- **Joins with other datasets**:
  - Links with orders to get purchase and delivery dates.
  - Links with payments to get installment information.

### `create_window_functions(orders_df, order_items_df, customers_df, products_df)`
- **Customer analytics**:
  - `cumulative_sales`: Running total of purchases by each unique customer.
  - `percent_of_total`: What percentage of lifetime spend each purchase represents.
  - `price_rank`: Ranks each purchase by price (highest = 1).
- **Product category analytics**:
  - Calculates delivery times grouped by product category.
  - Creates rolling averages with different window sizes (3, 7, 14 days).
  - Computes category mean delivery time.


## Other Transformation Functions

- `process_customers`: Standardizes city names to Title Case.
- `process_geolocation`: Removes duplicates and standardizes city names.
- `process_order_payments`: Fixes zero installments, standardizes payment types.
- `process_order_reviews`: Deduplicates reviews by ID, converts dates.
- `process_orders`: Converts text dates to datetime objects.
- `process_products`: Fixes column name misspellings, fills missing values.
- `process_sellers`: Standardizes city names.
- `process_category_translation`: Standardizes category names to lowercase with underscores.
 
 # Understanding `load_data.py`

## Key Functions in `load_data.py`

### `load_to_sql_server(processed_dfs, connection_string)`
- Extracts processed DataFrames and loads them into SQL Server tables.
- Creates dimension tables first, then the fact table.
- Implements **error handling** and sinking errors in a new table to keep track of issues.

### `create_fact_table(order_items_df, orders_df, processed_dfs, date_mapping)`

- **Core function** that builds the central fact table for analysis.

**Key transformations:**
- Handles missing delivery dates with default values.
- Selects only relevant columns needed for the final fact table.

## Key Dimension  
- **`create_date_dimension`**:  
  Builds the time dimension with attributes like year, month, quarter.

- **`create_customers_dimension`**:  
  Add customer data with geographic coordinates.

- **`create_products_dimension`**:  
  Maps product category names into English.

---

##  Warehouse Design

### Star Schema Approach 

- Uses **dimensional modeling**:  
  One central fact table connected to multiple dimension tables.
- Fact table contains **measures** (e.g., price, freight, delivery time) and **foreign keys**.
- Dimensions contain **descriptive attributes** based on business entities.

# Data Validation and Schema Management

## `olist_model.py`: Data Validation Framework

The `olist_model.py` file implements a validation layer using **Pandera** to enforce data quality rules on incoming data.

## `data_schemas.py`: CSV Import Specifications

This file defines the schema specifications for reading each CSV file correctly:

## `environment.yml`: Dependency Management
- `pandas==2.2.3` – Core library for data manipulation and analysis.
- `pandera==0.23.1` – Framework for declarative data validation.
- `pyodbc==5.2.0` – Enables connection to SQL Server databases.
- `sqlalchemy==2.0.39` – SQL toolkit and Object-Relational Mapping (ORM) for database operations.

## `main.py`: Orchestrate the whole data pipeline