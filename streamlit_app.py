import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col

# Ligar ao Snowflake com segredos
connection_parameters = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(connection_parameters).create()

# ⚠️ Corrigir uso de warehouse com hífen
session.sql('USE WAREHOUSE "COMPUTE-WH"').collect()
session.sql("USE DATABASE SMOOTHIES").collect()
session.sql("USE SCHEMA PUBLIC").collect()

# Título
st.title(f":cup_with_straw: Customize your Smoothie :cup_with_straw:")
st.write("Choose the fruits you want in your custom Smoothie!")

# Input do nome
name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be: ", name_on_order)

# Obter frutas e converter para lista
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
fruit_options = [row['FRUIT_NAME'] for row in my_dataframe.collect()]

# Multiselect com max 5 frutas
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    fruit_options,
    max_selections=5
)

# Inserção do pedido
if ingredients_list:
    ingredients_string = ' '.join(ingredients_list)

    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    time_to_insert = st.button('Submit order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")
