# How csv should be read and data type for each column
SCHEMAS = {
    'olist_customers_dataset.csv': {
        'dtype': {
            'customer_id': 'string',
            'customer_unique_id': 'string',
            'customer_zip_code_prefix': 'Int64',
            'customer_city': 'string',
            'customer_state': 'string'
        }
    },
    'olist_geolocation_dataset.csv': {
        'dtype': {
            'geolocation_zip_code_prefix': 'Int64',
            'geolocation_lat': 'float',
            'geolocation_lng': 'float',
            'geolocation_city': 'string',
            'geolocation_state': 'string'
        }
    },
    'olist_order_items_dataset.csv': {
        'dtype': {
            'order_id': 'string',
            'order_item_id': 'Int64',
            'product_id': 'string',
            'seller_id': 'string',
            'price': 'float',
            'freight_value': 'float'
        },
        'parse_dates': ['shipping_limit_date']
    },
    'olist_order_payments_dataset.csv': {
        'dtype': {
            'order_id': 'string',
            'payment_sequential': 'Int64',
            'payment_type': 'string',
            'payment_installments': 'Int64',
            'payment_value': 'float'
        }
    },
    'olist_order_reviews_dataset.csv': {
        'dtype': {
            'review_id': 'string',
            'order_id': 'string',
            'review_score': 'Int64',
            'review_comment_title': 'string',
            'review_comment_message': 'string'
        },
        'parse_dates': ['review_creation_date', 'review_answer_timestamp']
    },
    'olist_orders_dataset.csv': {
        'dtype': {
            'order_id': 'string',
            'customer_id': 'string',
            'order_status': 'string'
        },
        'parse_dates': [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]
    },
    'olist_products_dataset.csv': {
        'dtype': {
            'product_id': 'string',
            'product_category_name': 'string',
            'product_name_length': 'float',
            'product_description_length': 'float',
            'product_photos_qty': 'Int64',
            'product_weight_g': 'Int64',
            'product_length_cm': 'Int64',
            'product_height_cm': 'Int64',
            'product_width_cm': 'Int64'
        }
    },
    'olist_sellers_dataset.csv': {
        'dtype': {
            'seller_id': 'string',
            'seller_zip_code_prefix': 'Int64',
            'seller_city': 'string',
            'seller_state': 'string'
        }
    },
    'product_category_name_translation.csv': {
        'dtype': {
            'product_category_name': 'string',
            'product_category_name_english': 'string'
        }
    }
}
