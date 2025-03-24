import warnings
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

from src.models.data_schemas import SCHEMAS
from src.models.olist_model import (
    OlistCustomersModel,
    OlistGeolocationModel,
    OlistOrderItemsModel,
    OlistOrderPaymentsModel,
    OlistOrderReviewsModel,
    OlistOrdersModel,
    OlistProductsModel,
    OlistSellersModel,
    ProductCategoryNameTranslationModel
)

warnings.filterwarnings('ignore')

# Directory settings
INPUT_DIR = Path('../data/raw')
OUTPUT_DIR = Path('../data/processed')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging():
    log_dir = Path('../logs')
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'etl_process_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger()


def create_window_functions(orders_df, order_items_df, customers_df, products_df):
    results = {}


    # More efficient implementation with direct joins
    customer_orders = (
        order_items_df
        .merge(orders_df[['order_id', 'customer_id']], on='order_id')
        .merge(customers_df[['customer_id', 'customer_unique_id']], on='customer_id')
        .sort_values(['customer_unique_id', 'shipping_limit_date'])
    )

    # Calculate cumulative sales per customer
    customer_orders['cumulative_sales'] = customer_orders.groupby('customer_unique_id')['price'].cumsum()

    # Calculate percentage of total customer spend and order rank by price
    customer_orders['total_customer_sales'] = customer_orders.groupby('customer_unique_id')['price'].transform('sum')
    customer_orders['percent_of_total'] = (
            customer_orders['cumulative_sales'] / customer_orders['total_customer_sales'] * 100).round(2)
    customer_orders['price_rank'] = customer_orders.groupby('customer_unique_id')['price'].rank(method='dense',
                                                                                                ascending=False)

    results['customer_sales'] = customer_orders[[
        'customer_unique_id', 'order_id', 'price',
        'cumulative_sales', 'total_customer_sales', 'percent_of_total', 'price_rank'
    ]]

    # Calculate delivery time in days for valid orders
    orders_with_delivery = orders_df.dropna(subset=['order_delivered_customer_date', 'order_purchase_timestamp']).copy()
    orders_with_delivery['delivery_time_days'] = (
            (orders_with_delivery['order_delivered_customer_date'] - orders_with_delivery['order_purchase_timestamp'])
            .dt.total_seconds() / (60 * 60 * 24)
    )


    valid_orders = orders_with_delivery[orders_with_delivery['delivery_time_days'] > 0]

    # Join product categories with order items and delivery times
    category_delivery = (
        order_items_df[['order_id', 'product_id']]
        .merge(products_df[['product_id', 'product_category_name']], on='product_id')
        .merge(valid_orders[['order_id', 'delivery_time_days']], on='order_id')
    )

    category_delivery = category_delivery.sort_values(['product_category_name', 'delivery_time_days'])

    # Calculate rolling averages with different window sizes
    for window_size in [3, 7, 14]:
        column_name = f'rolling_avg_{window_size}d'
        category_delivery[column_name] = (
            category_delivery
            .groupby('product_category_name')['delivery_time_days']
            .transform(lambda x: x.rolling(window=window_size, min_periods=1).mean().round(2))
        )


    category_delivery['category_mean'] = (
        category_delivery
        .groupby('product_category_name')['delivery_time_days']
        .transform('mean')
        .round(2)
    )

    results['category_delivery_time'] = category_delivery

    return results


def process_customers(df):
    df['customer_city'] = df['customer_city'].str.title()
    return OlistCustomersModel.validate(df)


def process_geolocation(df):
    df = df.drop_duplicates()
    df['geolocation_city'] = df['geolocation_city'].str.title()
    return OlistGeolocationModel.validate(df)


def process_order_items(df, orders_df=None, payments_df=None):
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'])
    validated_df = OlistOrderItemsModel.validate(df)
    validated_df['total_price'] = validated_df['price'] + validated_df['freight_value']
    validated_df['profit_margin'] = validated_df['price'] - validated_df['freight_value']

    if orders_df is not None:
        orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
        orders_df['order_delivered_customer_date'] = pd.to_datetime(orders_df['order_delivered_customer_date'])
        orders_slim = orders_df[['order_id', 'order_purchase_timestamp', 'order_delivered_customer_date']]
        merged_df = pd.merge(validated_df, orders_slim, on='order_id', how='left')
        merged_df['delivery_time_days'] = (
                (merged_df['order_delivered_customer_date'] - merged_df['order_purchase_timestamp'])
                .dt.total_seconds() / (60 * 60 * 24)
        )
        validated_df = merged_df.drop(['order_purchase_timestamp', 'order_delivered_customer_date'], axis=1)

    if payments_df is not None:
        payment_counts = payments_df.groupby('order_id')['payment_installments'].sum().reset_index()
        validated_df = pd.merge(validated_df, payment_counts, on='order_id', how='left')

    return validated_df


def process_order_payments(df):
    df.loc[df['payment_installments'] == 0, 'payment_installments'] = 1
    df.loc[df['payment_type'].isnull(), 'payment_type'] = 'not_defined'
    return OlistOrderPaymentsModel.validate(df)


def process_order_reviews(df):
    df = df.drop_duplicates(subset=['review_id'], keep='first')
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'])
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'])
    df['review_comment_title'] = df['review_comment_title'].fillna('')
    df['review_comment_message'] = df['review_comment_message'].fillna('')
    validated_df = OlistOrderReviewsModel.validate(df)

    return validated_df


def process_orders(df):
    date_columns = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return OlistOrdersModel.validate(df)


def process_products(df):
    df.rename(columns={
        'product_name_lenght': 'product_name_length',
        'product_description_lenght': 'product_description_length'
    }, inplace=True)
    dimension_cols = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    df = df.dropna(subset=dimension_cols)
    df['product_category_name'] = df['product_category_name'].fillna('unknown')
    df['product_name_length'] = df['product_name_length'].fillna(df['product_name_length'].median())
    df['product_description_length'] = df['product_description_length'].fillna(
        df['product_description_length'].median())
    df['product_photos_qty'] = df['product_photos_qty'].fillna(1)
    return OlistProductsModel.validate(df)


def process_sellers(df):
    df['seller_city'] = df['seller_city'].str.title()
    return OlistSellersModel.validate(df)


def process_category_translation(df):
    df = df.drop_duplicates()
    df['product_category_name'] = df['product_category_name'].fillna('unknown')
    df['product_category_name'] = df['product_category_name'].str.lower().str.replace(' ', '_')
    df['product_category_name_english'] = df['product_category_name_english'].str.lower().str.replace(' ', '_')
    return ProductCategoryNameTranslationModel.validate(df)


def run_etl(logger=None):
    if logger is None:
        logger = setup_logging()

    processed_dfs = {}
    base_datasets = [
        'olist_customers_dataset.csv',
        'olist_geolocation_dataset.csv',
        'olist_order_payments_dataset.csv',
        'olist_order_reviews_dataset.csv',
        'olist_orders_dataset.csv',
        'olist_products_dataset.csv',
        'olist_sellers_dataset.csv',
        'product_category_name_translation.csv'
    ]

    for filename in base_datasets:
        logger.info(f"Processing {filename}...")
        try:
            schema = SCHEMAS[filename]
            dtype = schema.get('dtype', {})
            parse_dates = schema.get('parse_dates', None)
            input_file = INPUT_DIR / filename
            df = pd.read_csv(input_file, dtype=dtype, parse_dates=parse_dates)

            if filename == 'olist_customers_dataset.csv':
                processed_df = process_customers(df)
            elif filename == 'olist_geolocation_dataset.csv':
                processed_df = process_geolocation(df)
            elif filename == 'olist_order_payments_dataset.csv':
                processed_df = process_order_payments(df)
            elif filename == 'olist_order_reviews_dataset.csv':
                processed_df = process_order_reviews(df)
            elif filename == 'olist_orders_dataset.csv':
                processed_df = process_orders(df)
            elif filename == 'olist_products_dataset.csv':
                processed_df = process_products(df)
            elif filename == 'olist_sellers_dataset.csv':
                processed_df = process_sellers(df)
            elif filename == 'product_category_name_translation.csv':
                processed_df = process_category_translation(df)

            processed_dfs[filename] = processed_df
            output_file = OUTPUT_DIR / f"clean_{filename}"
            processed_df.to_csv(output_file, index=False)
            logger.info(f"Saved {output_file}")
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}", exc_info=True)

    logger.info("Processing order_items dataset...")
    try:
        if 'olist_orders_dataset.csv' in processed_dfs and 'olist_order_payments_dataset.csv' in processed_dfs:
            schema = SCHEMAS['olist_order_items_dataset.csv']
            input_file = INPUT_DIR / 'olist_order_items_dataset.csv'
            order_items_df = pd.read_csv(input_file, dtype=schema.get('dtype', {}),
                                         parse_dates=schema.get('parse_dates', None))
            processed_order_items = process_order_items(
                order_items_df,
                processed_dfs['olist_orders_dataset.csv'],
                processed_dfs['olist_order_payments_dataset.csv']
            )
            output_file = OUTPUT_DIR / "clean_olist_order_items_dataset.csv"
            processed_order_items.to_csv(output_file, index=False)
            logger.info(f"Saved {output_file}")
            processed_dfs['olist_order_items_dataset.csv'] = processed_order_items
        else:
            logger.warning("Missing required datasets for order_items processing")
    except Exception as e:
        logger.error(f"Error processing order_items: {e}", exc_info=True)

    logger.info("Creating window functions...")
    try:
        required = ['olist_orders_dataset.csv', 'olist_order_items_dataset.csv', 'olist_customers_dataset.csv',
                    'olist_products_dataset.csv']
        if all(ds in processed_dfs for ds in required):
            window_results = create_window_functions(
                processed_dfs['olist_orders_dataset.csv'],
                processed_dfs['olist_order_items_dataset.csv'],
                processed_dfs['olist_customers_dataset.csv'],
                processed_dfs['olist_products_dataset.csv']
            )
            for name, df in window_results.items():
                output_file = OUTPUT_DIR / f"window_{name}.csv"
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {output_file}")
        else:
            logger.warning("Missing required datasets for window functions")
    except Exception as e:
        logger.error(f"Error creating window functions: {e}", exc_info=True)

    logger.info("ETL process completed successfully!")
    return processed_dfs


if __name__ == "__main__":
    logger = setup_logging()
    run_etl(logger)