import pandas as pd
from datetime import datetime

def clean_numerical_column(df, column):
    if column not in df.columns:
        print(f"Column '{column}' not found in DataFrame.")
        return df
    
    if not pd.api.types.is_numeric_dtype(df[column]):
        print(f"Column '{column}' is not numeric.")
        return df
    
    if (df[column] < 0).any():
        print(f"Negative values in the '{column}' column have been detected. They will be deleted.")
        df = df[df[column] >= 0]
    
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    df_filtered = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    
    print(f"The outliers and null values for column '{column}' have been removed.")
    return df_filtered


def clean_datetime_columns(df, date_columns):
    current_date = datetime.now()
    for column in date_columns:
        
        df[column] = pd.to_datetime(df[column], errors='coerce')
        
        invalid_dates = df[column].isna().sum()
        if invalid_dates > 0:
            print(f"Detected {invalid_dates} invalid values in '{column}', they will be removed.")
            df = df.dropna(subset=[column])
        
        future_dates = (df[column] > current_date).sum()
        if future_dates > 0:
            print(f"Detected {future_dates} dates from the future in '{column}', they will be removed.")
            df = df[df[column] <= current_date]
    
    print("All date columns have been checked, incorrect and future values have been removed.")
    return df



def replace_ids_with_numbers(df, column_name):
   
    unique_ids = df[column_name].dropna().unique()
   
    id_to_number_mapping = {id_value: idx + 1 for idx, id_value in enumerate(unique_ids) if id_value}
    
    df[column_name] = df[column_name].map(id_to_number_mapping).fillna(df[column_name])
    
    return df


def convert_state_abbreviations(df, column):
    state_conversion = {
        'AC': 'Acre',
        'AL': 'Alagoas',
        'AP': 'Amapá',
        'AM': 'Amazonas',
        'BA': 'Bahia',
        'CE': 'Ceará',
        'DF': 'Distrito Federal',
        'ES': 'Espírito Santo',
        'GO': 'Goiás',
        'MA': 'Maranhão',
        'MT': 'Mato Grosso',
        'MS': 'Mato Grosso do Sul',
        'MG': 'Minas Gerais',
        'PA': 'Pará',
        'PB': 'Paraíba',
        'PR': 'Paraná',
        'PE': 'Pernambuco',
        'PI': 'Piauí',
        'RJ': 'Rio de Janeiro',
        'RN': 'Rio Grande do Norte',
        'RS': 'Rio Grande do Sul',
        'RO': 'Rondônia',
        'RR': 'Roraima',
        'SC': 'Santa Catarina',
        'SP': 'São Paulo',
        'SE': 'Sergipe',
        'TO': 'Tocantins'
    }

    if column in df.columns:
        df[column] = df[column].map(state_conversion)
        print(f"Abbreviations in the '{column}' column have been replaced with full names.")
    else:
        print(f"Column '{column}' not found in DataFrame.")

def capitalize_column(df, column):
    if column in df.columns:

        df[column] = df[column].astype(str).str.capitalize()
        print(f"Values in the '{column}' column now start with a capital letter.")
    else:
        print(f"Column '{column}' not found in DataFrame.")



customers = pd.read_csv("./data/olist_customers_dataset.csv", usecols=["customer_id", "customer_state"])
order_items = pd.read_csv("./data/olist_order_items_dataset.csv", usecols=["order_id", "product_id", "price", "freight_value", "order_item_id"])
reviews = pd.read_csv("./data/olist_order_reviews_dataset.csv", usecols=["order_id", "review_score"])
orders = pd.read_csv("./data/olist_orders_dataset.csv", usecols=["order_id", "customer_id", "order_status", "order_purchase_timestamp",  
                                                                    "order_delivered_customer_date"])
products = pd.read_csv("./data/olist_products_dataset.csv", usecols=["product_id", "product_category_name"])
category_translation = pd.read_csv("./data/product_category_name_translation.csv", usecols=["product_category_name", "product_category_name_english"])


df = orders.merge(customers, on="customer_id", how="left")

df = df.merge(order_items, on="order_id", how="left")


df = df.merge(reviews, on="order_id", how="left")


df = df.merge(products, on="product_id", how="left")


df = df.merge(category_translation, on="product_category_name", how="left")


df['product_category_name'] = df['product_category_name_english'].combine_first(df['product_category_name'])
df = df.drop(columns=['product_category_name_english'])

print(f"Dataset is created.")
print(f"Dataset length: {len(df)}")

print(f"Duplicates droping...")
df = df.drop_duplicates()

status_counts = df['order_status'].value_counts()
print("Unique values qunatity order_status:")
print(status_counts)

df = df[df['order_status'].isin(['delivered'])]
df = df.drop(columns=['order_status'])

# price_variations = df.groupby('product_id')['price'].nunique()
# price_discrepancies = price_variations[price_variations > 1]

# if not price_discrepancies.empty:
#     print("Products with different price values:")
#     print(price_discrepancies)
# else:
#     print("All products have the same price for each 'product_id'.")


df['price'] = df.groupby('product_id')['price'].transform(lambda x: x.fillna(x.mode()[0] if not x.mode().empty else x.mean()))

df['freight_value'] = df['freight_value'].fillna(0)


# duplicates_in_order = df[df.duplicated(subset=['order_id', 'product_id'], keep=False) & df['order_item_id'].notna()]

# print("Records where the same product appears on an order with different item numbers:")
# print(duplicates_in_order)


df['count'] = df.groupby(['order_id', 'product_id'])['product_id'].transform('count')
df = df.drop(columns=['order_item_id'])
df = df.drop_duplicates()

clean_numerical_column(df, 'price')
clean_numerical_column(df, 'freight_value')

date_columns = [
    'order_purchase_timestamp', 
    'order_delivered_customer_date'
]

df = clean_datetime_columns(df, date_columns)
date_order_violations = (
    (df['order_purchase_timestamp'] > df['order_delivered_customer_date'])
)

violations_count = date_order_violations.sum()
if violations_count > 0:
    print(f"There are {violations_count} records detected that are out of date order. They will be deleted.")
    df = df[~date_order_violations]
else:
    print("The order of dates in the data is observed.")

replace_ids_with_numbers(df, 'order_id')
if 'product_category_name' in df.columns:
    df['product_category_name'] = df['product_category_name'].fillna("no category").str.replace('_', ' ')

convert_state_abbreviations(df, 'customer_state')

capitalize_column(df, 'product_category_name')
capitalize_column(df, 'customer_city')

df = df.drop(columns=['product_id'])
df = df.drop(columns=['customer_id'])

df.to_csv("combined_dataset.csv", index=False)

print("Data successfully merged and saved in 'combined_dataset.csv'")