import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
import os


spark = SparkSession.builder.appName('spark_project')\
        .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
        .getOrCreate()


customer_data_path = "./Data/olist_customers_dataset.csv"
order_item_path = "./Data/olist_order_items_dataset.csv"
order_payment_path = "./Data/olist_order_payments_dataset.csv"
product_category_translation_path= "./Data/product_category_name_translation.csv"
product_path = './Data/olist_products_dataset.csv'
seller_path = './Data/olist_sellers_dataset.csv'
geolocation_path = './Data/olist_geolocation_dataset.csv'
orders_path = './Data/olist_orders_dataset.csv'
reviews = './Data/olist_order_reviews_dataset.csv'


reviews_df = spark.read.csv(reviews, header=True, inferSchema=True)
customer_df = spark.read.csv(customer_data_path, header=True, inferSchema=True)
order_item_df = spark.read.csv(order_item_path, header=True, inferSchema=True)
order_payment_df = spark.read.csv(order_payment_path, header=True, inferSchema=True)
product_category_translation_df = spark.read.csv(product_category_translation_path, header=True, inferSchema=True)
seller_df_uncleaned = spark.read.csv(seller_path, header=True, inferSchema=True)
product_df_uncleaned = spark.read.csv(product_path, header=True, inferSchema=True)
geoloacation_df_uncleaned = spark.read.csv(geolocation_path, header=True, inferSchema= True)
orders_df_uncleaned = spark.read.csv(orders_path, header=True, inferSchema= True)

"""
Cleaning Process:
For those data where cleaning is not needed directly saved to postgres.
"""

seller_df_uncleaned.select([f.trim(f.col(c)).alias(c) for c in seller_df_uncleaned.columns])

# Remove whitespace characters between words in all columns
seller_df = seller_df_uncleaned.select([f.regexp_replace(f.col(c), r'\s+', ' ').alias(c) for c in seller_df_uncleaned.columns])


status_counts = orders_df_uncleaned.groupBy("order_status").count()

# unavailable status vako order lai filter gareko
unavailableDrop = orders_df_uncleaned.filter(status_counts["order_status"] != "unavailable")

check = unavailableDrop.groupBy("order_status").count()


columns_to_check = ["order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date"]

grouped_null_counts = unavailableDrop.groupBy("order_status").agg(
    *[f.sum(f.col(c).isNull().cast("int")).alias(f"{c}_null_count") for c in columns_to_check]
)

delivered_drop = unavailableDrop.filter(~(f.col("order_status") == "delivered") | f.col("order_approved_at").isNotNull() |  f.col("order_delivered_carrier_date").isNotNull() | f.col("order_delivered_customer_date").isNotNull())

columns_to_check = ["order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date"]

grouped_null_counts = delivered_drop.groupBy("order_status").agg(
    *[f.sum(f.col(c).isNull().cast("int")).alias(f"{c}_null_count") for c in columns_to_check]
)

orders_df = delivered_drop


"""
Product DF
product ko name lai english ma translate gareko.
"""

product_joined_df= product_df_uncleaned.join(product_category_translation_df, "Product_category_name", "left")
product_df = product_joined_df.drop("product_category_name")
product_df = product_df.withColumnRenamed("product_category_name_english", "product_category_name")
product_df = product_df.drop(f.col("product_name_lenght"),f.col("product_description_lenght"),f.col("product_photos_qty"))
product_df.select([f.count(f.when(f.isnan(c) | f.col(c).isNull(), c)).alias(c) for c in product_df.columns]).show()
products_df = product_df.filter(~(f.col("product_category_name").isNull()))
products_count =  products_df.groupBy(f.col("product_category_name")).count()

products_df.select([f.count(f.when(f.isnan(c) | f.col(c).isNull(), c)).alias(c) for c in products_df.columns]).show()

product_df = products_df.filter(~f.col('product_weight_g').isNull())


"""
Geolocation Location Dataframe
"""
geoloacation_df_uncleaned.select('geolocation_zip_code_prefix').groupBy(f.col('geolocation_zip_code_prefix')).agg(f.count(f.col('geolocation_zip_code_prefix'))).show()

geolocation_avg_df = geoloacation_df_uncleaned\
    .groupBy(f.col('geolocation_zip_code_prefix'))\
    .agg(f.avg(f.col('geolocation_lat')).alias('geolocation_lat'), f.avg(f.col('geolocation_lng')).alias('geolocation_lng'))\
    .orderBy(f.col('geolocation_zip_code_prefix'))

geolocation_df = geolocation_avg_df


# Loading the cleaned dataset into postgresql.
load_dotenv()

Pg_user = os.getenv("POSTGRES_USER")
Pg_password = os.getenv("POSTGRES_PASSWORD")

customer_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'customer_df', user=Pg_user,password=Pg_password).mode('overwrite').save()

order_item_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'order_item_df', user=Pg_user,password=Pg_password).mode('overwrite').save()

order_payment_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'order_payment_df', user=Pg_user,password=Pg_password).mode('overwrite').save()

seller_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'seller_df', user=Pg_user,password=Pg_password).mode('overwrite').save()

orders_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'orders_df', user=Pg_user,password=Pg_password).mode('overwrite').save()

product_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'product_df', user=Pg_user,password=Pg_password).mode('overwrite').save()

geolocation_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'geolocation_df', user=Pg_user,password=Pg_password).mode('overwrite').save()


## Data reading from postgresql and solving questions.

table_names = ["customer_df","seller_df","geolocation_df","order_item_df","order_payment_df","orders_df","product_df"]
table_dataframes = {}

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": Pg_user,
    "password": Pg_password,
    "driver": "org.postgresql.Driver"
}

for table_name in table_names:
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    table_dataframes[table_name] = df

customer_df = table_dataframes["customer_df"]
seller_df = table_dataframes["seller_df"]
geolocation_df = table_dataframes["geolocation_df"]
order_item_df = table_dataframes["order_item_df"]
order_payment_df = table_dataframes["order_payment_df"]
orders_df = table_dataframes["orders_df"]
product_df = table_dataframes["product_df"]


"""
### Metrics 1 
Finding the distance between the customer and seller for each order.
Finding the average distance that the sellers from each state have sold the products i.e.
the average radius of distance covered by sellers of each state
"""

geoloacation_df_cust = geolocation_df.selectExpr('geolocation_zip_code_prefix as geolocation_zip_code_prefix_cust', 'geolocation_lat as geolocation_lat_cust', 'geolocation_lng as geolocation_lng_cust')
geoloacation_df_sell = geolocation_df.selectExpr('geolocation_zip_code_prefix as geolocation_zip_code_prefix_sell', 'geolocation_lat as geolocation_lat_sell', 'geolocation_lng as geolocation_lng_sell')
jointype = 'inner'

geoloacation_df_cust_broadcast = f.broadcast(geoloacation_df_cust)
geoloacation_df_sell_broadcast = f.broadcast(geoloacation_df_sell)

single_joined_table = (
    customer_df.alias('c')
    .join(geoloacation_df_cust_broadcast.alias('gc'), f.col('c.customer_zip_code_prefix') == f.col('gc.geolocation_zip_code_prefix_cust'), jointype)
    .select(
        "c.customer_id",
        "c.customer_zip_code_prefix",
        "gc.geolocation_lat_cust",
        "gc.geolocation_lng_cust"
    )
    .join(orders_df.alias('o'), f.col('c.customer_id') == f.col('o.customer_id'), jointype)
    .join(order_item_df.alias('oi'), f.col('o.order_id') == f.col('oi.order_id'), jointype)
    .join(seller_df.alias('s'), f.col('oi.seller_id') == f.col('s.seller_id'), jointype)
    .join(geoloacation_df_sell_broadcast.alias('gs'), f.col('s.seller_zip_code_prefix') == f.col('gs.geolocation_zip_code_prefix_sell'), jointype)
    .select(
        "o.order_id",
        "oi.product_id",
        "c.customer_id",
        "c.customer_zip_code_prefix",
        "gc.geolocation_lat_cust",
        "gc.geolocation_lng_cust",
        "oi.seller_id",
        "s.seller_zip_code_prefix",
        "s.seller_state",
        "gs.geolocation_lat_sell",
        "gs.geolocation_lng_sell"
    )
)

# Convert latitude and longitude from degrees to radians
single_joined_table = single_joined_table.withColumn("geolocation_lat_cust_rad", f.radians(single_joined_table["geolocation_lat_cust"]).cast("double"))
single_joined_table = single_joined_table.withColumn("geolocation_lng_cust_rad", f.radians(single_joined_table["geolocation_lng_cust"]).cast("double"))
single_joined_table = single_joined_table.withColumn("geolocation_lat_sell_rad", f.radians(single_joined_table["geolocation_lat_sell"]).cast("double"))
single_joined_table = single_joined_table.withColumn("geolocation_lng_sell_rad", f.radians(single_joined_table["geolocation_lng_sell"]).cast("double"))

# Calculate the distance using the spherical law of cosines
single_joined_table = single_joined_table.withColumn(
    "distance_km",
    f.acos(
        f.sin("geolocation_lat_cust_rad") * f.sin("geolocation_lat_sell_rad") +
        f.cos("geolocation_lat_cust_rad") * f.cos("geolocation_lat_sell_rad") *
        f.cos(f.col("geolocation_lng_sell_rad") - f.col("geolocation_lng_cust_rad"))
    ).cast("double") * 6371.0  # Radius of the Earth in kilometers
)

# Drop the intermediate columns
single_joined_table = single_joined_table.drop(
    "geolocation_lat_cust_rad",
    "geolocation_lng_cust_rad",
    "geolocation_lat_sell_rad",
    "geolocation_lng_sell_rad"
)

# Show the DataFrame with the distance column
distance_output = single_joined_table.select("order_id","product_id","customer_id","seller_id","distance_km")
distance_output.show()

distance_output.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'seller_customer_distance', user = Pg_user,password=Pg_password).mode('overwrite').save()

sp_df = single_joined_table.filter((single_joined_table["seller_state"] == "SP") & single_joined_table["distance_km"].isNotNull() & (single_joined_table["distance_km"] > 0))
distance_sp = sp_df.withColumn('distance_km', f.col('distance_km').cast('int'))

state_analysis1 = distance_sp.agg(
    f.avg("distance_km").alias("avg_distance_km")
)

avg_distance_km = state_analysis1.collect()[0]["avg_distance_km"]

average_distance_by_state = single_joined_table.groupBy("seller_state")

state_analysis = average_distance_by_state.agg(
    f.avg("distance_km").alias("avg_distance_km"),
    f.count("product_id").alias("product_count")
)

state_analysis = state_analysis.na.fill(avg_distance_km, subset=["avg_distance_km"])

state_analysis.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'seller_average_distance', user=Pg_user,password=Pg_password).mode('overwrite').save()


### Metrics 2

joined_df = order_payment_df.join(single_joined_table, "order_id", "inner")\
                            .join(product_df.alias('p'),single_joined_table['product_id'] == product_df['product_id'])
joined_df = joined_df.withColumn("payment_value", f.coalesce(joined_df["payment_value"], f.lit(0)))


revenue_by_product_state = (
    joined_df
    .groupBy("seller_state", "product_category_name")
    .agg(f.sum("payment_value").alias("total_revenue"))
)

pivot_table = (
    revenue_by_product_state
    .groupBy("product_category_name")
    .pivot("seller_state")
    .agg(f.first("total_revenue"))  # You can choose any aggregation method that makes sense for your data
)

# Show the pivot table
pivot_table1 = pivot_table.fillna(0)
# pivot_table1.show()

pivot_table1.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'Question_2_pivot_1', user=Pg_user,password=Pg_password).mode('overwrite').save()

from pyspark.sql.window import Window

# Calculate total revenue by state and product category
revenue_by_product_state = (
    joined_df
    .groupBy("seller_state", "product_category_name")
    .agg(f.sum("payment_value").alias("total_revenue"))
)

# Create a window specification to partition by product category
window_spec = Window.partitionBy("seller_state")

# Calculate the total revenue for each product category within each state
revenue_by_product_state = revenue_by_product_state.withColumn(
    "total_revenue_category_state",
    f.sum("total_revenue").over(window_spec)
)

# Calculate the percentage of revenue for each product category in each state
revenue_by_product_state = revenue_by_product_state.withColumn(
    "percentage_of_revenue",
    (revenue_by_product_state["total_revenue"] / revenue_by_product_state["total_revenue_category_state"]) * 100
)

# Pivot the table to get state-wise percentages of revenue for each product
pivot_table = (
    revenue_by_product_state
    .groupBy("seller_state")
    .pivot("product_category_name")
    .agg(f.first("percentage_of_revenue"))
)

# Fill NaN values with 0
pivot_table2 = pivot_table.fillna(0)
pivot_table2.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'Question_2_pivot_2', user=Pg_user,password=Pg_password).mode('overwrite').save()

spark.stop()

