import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime

# Ensuring that data is loaded correctly maintaining correct data type and constraints
class OlistBaseModel(pa.DataFrameModel):
    class Config:
        strict = True


class OlistCustomersModel(OlistBaseModel):
    customer_id: Series[pd.StringDtype] = pa.Field(nullable=False, unique=True)
    customer_unique_id: Series[pd.StringDtype] = pa.Field(nullable=False)
    customer_zip_code_prefix: Series[int] = pa.Field(nullable=True, ge=1000)
    customer_city: Series[pd.StringDtype] = pa.Field(nullable=False)
    customer_state: Series[pd.StringDtype] = pa.Field(nullable=False)


class OlistGeolocationModel(OlistBaseModel):
    geolocation_zip_code_prefix: Series[int] = pa.Field(nullable=True, ge=1000)
    geolocation_lat: Series[float] = pa.Field(nullable=False, ge=-90.0, le=90.0)
    geolocation_lng: Series[float] = pa.Field(nullable=False, ge=-180.0, le=180.0)
    geolocation_city: Series[pd.StringDtype] = pa.Field(nullable=False)
    geolocation_state: Series[pd.StringDtype] = pa.Field(nullable=False)


class OlistOrderItemsModel(OlistBaseModel):
    order_id: Series[pd.StringDtype] = pa.Field(nullable=False)
    order_item_id: Series[int] = pa.Field(nullable=True, ge=1)
    product_id: Series[pd.StringDtype] = pa.Field(nullable=False)
    seller_id: Series[pd.StringDtype] = pa.Field(nullable=False)
    shipping_limit_date: Series[DateTime] = pa.Field(nullable=False)
    price: Series[float] = pa.Field(nullable=False, ge=0.0)
    freight_value: Series[float] = pa.Field(nullable=False, ge=0.0)


class OlistOrderPaymentsModel(OlistBaseModel):
    order_id: Series[pd.StringDtype] = pa.Field(nullable=False)
    payment_sequential: Series[int] = pa.Field(nullable=True, ge=1)
    payment_type: Series[pd.StringDtype] = pa.Field(nullable=False,
                                                    isin=["credit_card", "boleto", "voucher", "debit_card",
                                                          "not_defined"])
    payment_installments: Series[int] = pa.Field(nullable=True, ge=1)
    payment_value: Series[float] = pa.Field(nullable=False, ge=0.0)


class OlistOrderReviewsModel(OlistBaseModel):
    review_id: Series[pd.StringDtype] = pa.Field(nullable=False, unique=True)
    order_id: Series[pd.StringDtype] = pa.Field(nullable=False)
    review_score: Series[int] = pa.Field(nullable=True, ge=1, le=5)
    review_comment_title: Series[pd.StringDtype] = pa.Field(nullable=True)
    review_comment_message: Series[pd.StringDtype] = pa.Field(nullable=True)
    review_creation_date: Series[DateTime] = pa.Field(nullable=False)
    review_answer_timestamp: Series[DateTime] = pa.Field(nullable=True)


class OlistOrdersModel(OlistBaseModel):
    order_id: Series[pd.StringDtype] = pa.Field(nullable=False, unique=True)
    customer_id: Series[pd.StringDtype] = pa.Field(nullable=False)
    order_status: Series[pd.StringDtype] = pa.Field(nullable=False)
    order_purchase_timestamp: Series[DateTime] = pa.Field(nullable=False)
    order_approved_at: Series[DateTime] = pa.Field(nullable=True)
    order_delivered_carrier_date: Series[DateTime] = pa.Field(nullable=True)
    order_delivered_customer_date: Series[DateTime] = pa.Field(nullable=True)
    order_estimated_delivery_date: Series[DateTime] = pa.Field(nullable=False)


class OlistProductsModel(OlistBaseModel):
    product_id: Series[pd.StringDtype] = pa.Field(nullable=False, unique=True)
    product_category_name: Series[pd.StringDtype] = pa.Field(nullable=True)
    product_name_length: Series[float] = pa.Field(nullable=True, ge=0)
    product_description_length: Series[float] = pa.Field(nullable=True, ge=0)
    product_photos_qty: Series[int] = pa.Field(nullable=True, ge=0)
    product_weight_g: Series[int] = pa.Field(nullable=True, ge=0)
    product_length_cm: Series[int] = pa.Field(nullable=True, ge=0)
    product_height_cm: Series[int] = pa.Field(nullable=True, ge=0)
    product_width_cm: Series[int] = pa.Field(nullable=True, ge=0)


class OlistSellersModel(OlistBaseModel):
    seller_id: Series[pd.StringDtype] = pa.Field(nullable=False, unique=True)
    seller_zip_code_prefix: Series[int] = pa.Field(nullable=True, ge=1000)
    seller_city: Series[pd.StringDtype] = pa.Field(nullable=False)
    seller_state: Series[pd.StringDtype] = pa.Field(nullable=False)


class ProductCategoryNameTranslationModel(OlistBaseModel):
    product_category_name: Series[pd.StringDtype] = pa.Field(nullable=False, unique=True)
    product_category_name_english: Series[pd.StringDtype] = pa.Field(nullable=False)
