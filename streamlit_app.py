import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, when_matched

# Conectar ao Snowflake com segredos do Streamlit
connection_parameters = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(connection_parameters).create()

# ⚙️ Definir o contexto Snowflake corretamente
session.sql('USE WAREHOUSE "COMPUTE_WH"').collect()
session.sql("USE DATABASE SMOOTHIES").collect()
session.sql("USE SCHEMA PUBLIC").collect()

# 🎨 Título da aplicação
st.title(":cup_with_straw: Customize your Smoothie :cup_with_straw:")
st.write("Choose the fruits you want in your custom Smoothie!")

# ✏️ Nome do cliente
name_on_order = st.text_input("Name on Smoothie:")
if name_on_order:
    st.write("The name on your Smoothie will be:", name_on_order)

# 🍓 Obter frutas disponíveis
try:
    fruit_df = session.table("FRUIT_OPTIONS").select(col("FRUIT_NAME"))
    fruit_options = [row["FRUIT_NAME"] for row in fruit_df.collect()]
except Exception as e:
    st.error("Erro ao carregar as frutas disponíveis.")
    st.exception(e)
    st.stop()

# 🧃 Multiselect com máximo de 5 frutas
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)

# 📤 Submissão da encomenda
if ingredients_list and name_on_order:
    ingredients_string = ' '.join(ingredients_list)

    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    if st.button("Submit order"):
        try:
            session.sql(my_insert_stmt).collect()
            st.success("✅ Your Smoothie is ordered!")
        except Exception as e:
            st.error("Erro ao submeter o pedido.")
            st.exception(e)
else:
    st.info("Please select at least one ingredient and enter your name.")

