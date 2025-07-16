import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# 🧠 Função para normalizar os nomes das frutas da base de dados para nomes da API Fruityvice
def normalize_fruit_name(fruit):
    fruit = fruit.lower().strip()
    if fruit.endswith("ies"):
        fruit = fruit[:-3] + "y"  # Ex: "Strawberries" → "Strawberry"
    elif fruit.endswith("s"):
        fruit = fruit[:-1]  # Ex: "Apples" → "Apple"
    return fruit.replace(" ", "")  # remove espaços, ex: "Dragon Fruit" → "dragonfruit"

# 🔐 Conectar ao Snowflake com segredos do Streamlit
connection_parameters = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(connection_parameters).create()

# ⚙️ Definir o contexto Snowflake corretamente
session.sql('USE WAREHOUSE "COMPUTE_WH"').collect()
session.sql("USE DATABASE SMOOTHIES").collect()
session.sql("USE SCHEMA PUBLIC").collect()

# 🎨 UI da aplicação
st.title(":cup_with_straw: Customize your Smoothie :cup_with_straw:")
st.write("Choose the fruits you want in your custom Smoothie!")

name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be:", name_on_order)

# 🍓 Obter frutas disponíveis a partir da tabela Snowflake
cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    my_dataframe,
    max_selections=5
)

# 🧾 Mostrar nutrição + registar pedido
if ingredients_list:
    ingredients_string = ""

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + " "

        st.subheader(f"{fruit_chosen} Nutrition Information")
        api_fruit_name = normalize_fruit_name(fruit_chosen)
        url = f"https://fruityvice.com/api/fruit/{api_fruit_name}"
        response = requests.get(url)

        if response.status_code == 200:
            fruit_data = response.json()
            nutrition_data = fruit_data["nutritions"]
            nutrition_df = pd.DataFrame([nutrition_data], index=[fruit_data["name"]])
            st.dataframe(nutrition_df, use_container_width=True)
        else:
            st.warning(f"Não foi possível obter dados de '{fruit_chosen}'. Verifica se existe na API Fruityvice.")

    # Enviar para Snowflake
    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{ingredients_string.strip()}', '{name_on_order}')
    """

    if st.button('Submit order'):
        session.sql(my_insert_stmt).collect()
        st.success("Your Smoothie is ordered!", icon="✅")

