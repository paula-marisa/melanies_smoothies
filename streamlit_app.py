import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, when_matched
import requests
import pandas as pd

# Conectar ao Snowflake com segredos do Streamlit
connection_parameters = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(connection_parameters).create()

# ⚙️ Definir o contexto Snowflake corretamente
session.sql('USE WAREHOUSE "COMPUTE_WH"').collect()
session.sql("USE DATABASE SMOOTHIES").collect()
session.sql("USE SCHEMA PUBLIC").collect()

# Write directly to the app
st.title(f":cup_with_straw: Customize your Smoothie :cup_with_straw:")
st.write(
  """Choose the fruits you want in your custom Smoothie!
  """
)

name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be: ", name_on_order)

#session = get_active_session()
cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:'
    , my_dataframe
    , max_selections=5
)

if ingredients_list:
    ingredients_string = ''

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
        st.subheader(fruit_chosen + 'Nutrition Information')
        api_fruit_name = fruit_chosen.lower().replace(" ", "_")
        smoothiefroot_response = requests.get(f"https://fruityvice.com/api/fruit/{api_fruit_name}")
        if smoothiefroot_response.status_code == 200:
          fruit_data = smoothiefroot_response.json()
          nutrition_data = fruit_data["nutritions"]
          nutrition_df = pd.DataFrame([nutrition_data], index=[fruit_data["name"]])
          st.dataframe(nutrition_df, use_container_width=True)
        else:
          st.warning(f"Não foi possível obter dados de '{fruit_chosen}'. Verifica se existe na API Fruityvice.")
    
    my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order)
            values ('""" + ingredients_string + """', '""" + name_on_order + """')"""

    #st.write(my_insert_stmt)
    #st.stop()

    time_to_insert = st.button('Submit order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()

        st.success('Your Smoothie is ordered!', icon="✅")
