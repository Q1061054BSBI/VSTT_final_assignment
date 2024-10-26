import pandas as pd
from datetime import datetime

def clean_numerical_column(df, column):
    # Проверка существования столбца
    if column not in df.columns:
        print(f"Столбец '{column}' не найден в DataFrame.")
        return df
    
    # Проверка, является ли столбец числовым
    if not pd.api.types.is_numeric_dtype(df[column]):
        print(f"Столбец '{column}' не является числовым.")
        return df
    
    # Проверка на наличие отрицательных значений
    if (df[column] < 0).any():
        print(f"Обнаружены отрицательные значения в столбце '{column}'. Они будут удалены.")
        df = df[df[column] >= 0]
    
    # Вычисление межквартильного размаха (IQR) и границ для определения выбросов
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    # Фильтрация данных для удаления выбросов
    df_filtered = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    
    print(f"Выбросы и нулевые значения для столбца '{column}' удалены.")
    return df_filtered


def clean_datetime_columns(df, date_columns):
    current_date = datetime.now()
    for column in date_columns:
        # Преобразуем столбец в формат datetime с параметром errors='coerce' для замены некорректных значений на NaT
        df[column] = pd.to_datetime(df[column], errors='coerce')
        
        # Удаляем строки с некорректными датами (NaT)
        invalid_dates = df[column].isna().sum()
        if invalid_dates > 0:
            print(f"Обнаружено {invalid_dates} некорректных значений в '{column}', они будут удалены.")
            df = df.dropna(subset=[column])
        
        # Удаляем строки с будущими датами
        future_dates = (df[column] > current_date).sum()
        if future_dates > 0:
            print(f"Обнаружено {future_dates} дат из будущего в '{column}', они будут удалены.")
            df = df[df[column] <= current_date]
    
    print("Все столбцы с датами проверены, некорректные и будущие значения удалены.")
    return df



def replace_ids_with_numbers(df, column_name):
    """
    Заменяет уникальные значения ID в указанном столбце на последовательные числа, кроме пустых значений.

    :param df: DataFrame, в котором находятся данные.
    :param column_name: Название столбца, в котором нужно произвести замену.
    :return: DataFrame с заменёнными значениями ID.
    """
    # Получаем уникальные непустые значения в столбце
    unique_ids = df[column_name].dropna().unique()
    
    # Создаём словарь, который сопоставляет каждый уникальный ID с числом
    id_to_number_mapping = {id_value: idx + 1 for idx, id_value in enumerate(unique_ids) if id_value}
    
    # Применяем замену значений с помощью map, оставляя пустые значения без изменений
    df[column_name] = df[column_name].map(id_to_number_mapping).fillna(df[column_name])
    
    return df


def convert_state_abbreviations(df, column):
    # Полный словарь для преобразования аббревиатур штатов Бразилии в полные названия
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

    # Замена значений в указанном столбце
    if column in df.columns:
        df[column] = df[column].map(state_conversion)
        print(f"Аббревиатуры в столбце '{column}' заменены на полные названия.")
    else:
        print(f"Столбец '{column}' не найден в DataFrame.")

def capitalize_column(df, column):
    # Проверка, существует ли указанный столбец в DataFrame
    if column in df.columns:
        # Преобразуем значения в столбце так, чтобы каждая строка начиналась с заглавной буквы
        df[column] = df[column].astype(str).str.capitalize()
        print(f"Значения в столбце '{column}' теперь начинаются с заглавной буквы.")
    else:
        print(f"Столбец '{column}' не найден в DataFrame.")


customers = pd.read_csv("./data/olist_customers_dataset.csv", usecols=["customer_id", "customer_city", "customer_state"])
order_items = pd.read_csv("./data/olist_order_items_dataset.csv", usecols=["order_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value", "order_item_id"])
reviews = pd.read_csv("./data/olist_order_reviews_dataset.csv", usecols=["order_id", "review_id", "review_score"])
orders = pd.read_csv("./data/olist_orders_dataset.csv", usecols=["order_id", "customer_id", "order_status", "order_purchase_timestamp", 
                                                                    "order_approved_at", "order_delivered_carrier_date", 
                                                                    "order_delivered_customer_date", "order_estimated_delivery_date"])
products = pd.read_csv("./data/olist_products_dataset.csv", usecols=["product_id", "product_category_name"])
sellers = pd.read_csv("./data/olist_sellers_dataset.csv", usecols=["seller_id", "seller_city", "seller_state"])
category_translation = pd.read_csv("./data/product_category_name_translation.csv", usecols=["product_category_name", "product_category_name_english"])


df = orders.merge(customers, on="customer_id", how="left")

df = df.merge(order_items, on="order_id", how="left")


df = df.merge(reviews, on="order_id", how="left")


df = df.merge(products, on="product_id", how="left")


df = df.merge(category_translation, on="product_category_name", how="left")


df = df.merge(sellers, on="seller_id", how="left")

df['product_category_name'] = df['product_category_name_english'].combine_first(df['product_category_name'])
df = df.drop(columns=['product_category_name_english'])

print(f"Dataset is created.")
print(f"Dataset length: {len(df)}")

print(f"Duplicates droping...")
df = df.drop_duplicates()
print(f"Dataset length: {len(df)}")

# unique_statuses = df['order_status'].unique()
# print("Уникальные значения в столбце order_status:", unique_statuses)
status_counts = df['order_status'].value_counts()
print("Unique values qunatity order_status:")
print(status_counts)

# Оставляем только записи со статусом 'delivered'
df = df[df['order_status'] == 'delivered']

# Удаляем столбец 'order_status'
df = df.drop(columns=['order_status'])

print(f"Dataset length: {len(df)}")

#хотим узнать есть ли какой то разброс по ценами на одни и те же продукты
# Группируем по 'product_id' и считаем уникальные значения в 'price'
price_variations = df.groupby('product_id')['price'].nunique()

# Находим 'product_id' с более чем одним уникальным значением цены
price_discrepancies = price_variations[price_variations > 1]


if not price_discrepancies.empty:
    print("Продукты с разными значениями цен:")
    print(price_discrepancies)
else:
    print("Все продукты имеют одинаковую цену для каждого 'product_id'.")


#Заполняем пустые значеия price модой
df['price'] = df.groupby('product_id')['price'].transform(lambda x: x.fillna(x.mode()[0] if not x.mode().empty else x.mean()))

df['freight_value'] = df['freight_value'].fillna(0)


## Проверка наличия одинаковых продуктов с разными порядковыми номерами в одном заказе
duplicates_in_order = df[df.duplicated(subset=['order_id', 'product_id'], keep=False) & df['order_item_id'].notna()]

print("Записи, где один и тот же продукт появляется в заказе с разными номерами позиций:")
print(duplicates_in_order)


# Подсчитываем количество повторений продукта в каждом заказе и записываем в новый столбец "Quantity"
df['count'] = df.groupby(['order_id', 'product_id'])['product_id'].transform('count')

# Удаляем столбец 'order_item_id'
df = df.drop(columns=['order_item_id'])

# Удаляем дубликаты, оставляя уникальные комбинации заказов и продуктов
df = df.drop_duplicates()

print(f"Dataset length: {len(df)}")

#удаляем выборосы из столбца цен
clean_numerical_column(df, 'price')

print(f"Dataset length: {len(df)}")

#удаляем выборосы из столбца стомости доставки
clean_numerical_column(df, 'freight_value')

print(f"Dataset length: {len(df)}")

date_columns = [
    'order_purchase_timestamp', 
    'order_approved_at', 
    'order_delivered_carrier_date', 
    'order_delivered_customer_date', 
    'order_estimated_delivery_date'
]

df = clean_datetime_columns(df, date_columns)

print(f"Dataset length: {len(df)}")


# Проверка соблюдения правильного порядка дат
date_order_violations = (
    (df['order_purchase_timestamp'] > df['order_approved_at']) |
    (df['order_approved_at'] > df['order_delivered_carrier_date']) |
    (df['order_delivered_carrier_date'] > df['order_delivered_customer_date'])
)

# Подсчет записей с нарушением порядка
violations_count = date_order_violations.sum()
if violations_count > 0:
    print(f"Обнаружено {violations_count} записей с нарушением порядка дат. Они будут удалены.")
    # Удаление записей с нарушенным порядком дат
    df = df[~date_order_violations]
else:
    print("Порядок дат в данных соблюден.")

print(f"Dataset length: {len(df)}")

#ids updated

replace_ids_with_numbers(df, 'order_id')
replace_ids_with_numbers(df, 'product_id')
replace_ids_with_numbers(df, 'customer_id')
replace_ids_with_numbers(df, 'review_id')
replace_ids_with_numbers(df, 'seller_id')


# Проверка и обработка столбца 'product_category_name'
if 'product_category_name' in df.columns:
    # Заменяем подчеркивания на пробелы и устанавливаем "no category" для пустых значений
    df['product_category_name'] = df['product_category_name'].fillna("no category").str.replace('_', ' ')


seller_state_counts = df['seller_state'].value_counts()
customer_state_counts = df['customer_state'].value_counts()

#Где больше штатов в seller state или в customer
if(len(seller_state_counts) > len(customer_state_counts)):
    print('Seller state quntity is bigger')
    print(seller_state_counts)
else:
    print('Customers state quntity is bigger')
    print(customer_state_counts)


# Конвертация аббревиатуры в полное название
convert_state_abbreviations(df, 'customer_state')
convert_state_abbreviations(df, 'seller_state')


#Capitalizing of text columns
capitalize_column(df, 'product_category_name')
capitalize_column(df, 'seller_city')
capitalize_column(df, 'customer_city')
#тут не преобраузем штаты, потому что в словаре аббревиатур и так все полные названия с большой буквы и мы это знает точно

df.to_csv("combined_dataset.csv", index=False)

print("Данные успешно объединены и сохранены в 'combined_dataset.csv'")