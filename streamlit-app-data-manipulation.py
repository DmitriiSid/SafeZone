import streamlit as st
import streamlit.components.v1 as components
from streamlit_card import card
from kbcstorage.client import Client
import os
import csv
import pandas as pd
import datetime

# Setting page config
st.set_page_config(page_title="Keboola Data Editor", page_icon=":robot:", layout="wide")

# Constants
token = st.secrets["kbc_storage_token"]
kbc_url = st.secrets["kbc_url"]
kbc_token = st.secrets["kbc_token"]

# Initialize Client
client = Client(kbc_url, token)
kbc_client = Client(kbc_url, kbc_token)

if 'data_load_time_table' not in st.session_state:
    st.session_state['data_load_time_table'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

if 'data_load_time_overview' not in st.session_state:
    st.session_state['data_load_time_overview'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Fetching data 
@st.cache_data(ttl=60, show_spinner=False)
def get_dataframe(table_id):
    table_detail = client.tables.detail(table_id)

    client.tables.export_to_file(table_id=table_id, path_name='')
    
    with open('./' + table_detail['name'], mode='rt', encoding='utf-8') as in_file:
        lazy_lines = (line.replace('\0', '') for line in in_file)
        reader = csv.reader(lazy_lines, lineterminator='\n')
    if os.path.exists('data.csv'):
        os.remove('data.csv')
    else:
        print("The file does not exist")
    
    os.rename(table_detail['name'], 'data.csv')
    df = pd.read_csv('data.csv')
    return df

# Initialization
def init():
    if 'selected-table' not in st.session_state:
        st.session_state['selected-table'] = None

    if 'tables_id' not in st.session_state:
        st.session_state['tables_id'] = pd.DataFrame(columns=['table_id'])
    
    if 'data' not in st.session_state:
        st.session_state['data'] = None 

    if 'upload-tables' not in st.session_state:
        st.session_state["upload-tables"] = False
    
    if 'log-exists' not in st.session_state:
        st.session_state["log-exists"] = False

def update_session_state(table_id):
    with st.spinner('Loading ...'):
        st.session_state['selected-table'] = table_id
        st.session_state['data'] = get_dataframe(st.session_state['selected-table'])
        st.session_state['data_load_time_table'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    st.rerun()

def display_footer_section():
    left_aligned, space_col, right_aligned = st.columns((2, 7, 1))
    with left_aligned:
        st.caption("© Keboola 2024")
    with right_aligned:
        st.caption("Version 2.0")

def write_to_keboola(data, table_id, table_path, incremental):
    """
    Writes the provided data to the specified table in Keboola Connection,
    updating existing records as needed.

    Args:
        data (pandas.DataFrame): The data to write to the table.
        table_id (str): The ID of the table to write the data to.
        table_path (str): The local file path to write the data to before uploading.

    Returns:
        None
    """

    # Write the DataFrame to a CSV file with compression
    data.to_csv(table_path, index=False, compression='gzip')

    # Load the CSV file into Keboola, updating existing records
    client.tables.load(
        table_id=table_id,
        file_path=table_path,
        is_incremental=incremental
    )

def resetSetting():
    st.session_state['selected-table'] = None
    st.session_state['data'] = None 

def write_to_log(data):
    now = datetime.datetime.now()
    log_df = pd.DataFrame({
        'table_id': "in.c-keboolasheets.log",
        'new': [data],
        'log_time': now,
        'user': "PlaceHolderUserID"
    })
    log_df.to_csv(f'updated_data_log.csv.gz', index=False, compression='gzip')

    # Load the CSV file into Keboola, updating existing records
    kbc_client.tables.load(
        table_id="in.c-keboolasheets.log",
        file_path=f'updated_data_log.csv.gz',
        is_incremental=True)

def cast_bool_columns(df):
    """Ensure that columns that should be boolean are explicitly cast to boolean."""
    for col in df.columns:
        # If a column in the DataFrame has only True/False or NaN values, cast it to bool
        if df[col].dropna().isin([True, False]).all():
            df[col] = df[col].astype(bool)
    return df

# Display tables
init()

# Specific table ID
table_id = "in.c-mapa-pomoci-input.db_pomoci"  # Zadejte správné ID tabulky zde
st.session_state['selected-table'] = table_id
st.session_state['data'] = get_dataframe(st.session_state['selected-table'])

col1, col2, col4 = st.columns((2, 7, 2))
with col4:
    st.markdown(f"**Data Freshness:** \n {st.session_state['data_load_time_table']}")

# Data Editor
st.title("Safezona - úprava dat pro Mapu pomoci")


# Reload Button
if st.button("Reload Data", key="reload-table", use_container_width=True):
    st.session_state['data'] = get_dataframe(st.session_state['selected-table'])
    st.toast('Data Reloaded!', icon="✅")

# Expander with info about table
with st.expander("Table Info"):
    # Displaying data in bold using Markdown
    st.markdown(f"**Table ID:** {table_id}")

# Form for adding new rows
st.subheader("Přidat nový řádek")
with st.form(key="add_row_form"):
    new_row_data = {}
    for column in st.session_state['data'].columns:
        new_row_data[column] = st.text_input(column)
    
    submit_button = st.form_submit_button(label="Add Row")
    if submit_button:
        # Check for duplicate 'Nazev' in the DataFrame
        existing_names = st.session_state["data"]["Nazev"].values
        if new_row_data["Nazev"] in existing_names:
            st.error("Řádek s tímto názvem již existuje, nový řádek nelze přidat.")
        else:
            new_row_df = pd.DataFrame([new_row_data])
            st.session_state["data"] = pd.concat([st.session_state["data"], new_row_df], ignore_index=True)
            st.success('New row added!', icon="✅")
            # Save data immediately after adding new row
            with st.spinner('Saving Data...'):
                kbc_data = cast_bool_columns(get_dataframe(st.session_state["selected-table"]))
                st.session_state["data"] = cast_bool_columns(st.session_state["data"])
                is_incremental = False  # Change to True if primary keys are set
                write_to_keboola(st.session_state["data"], st.session_state["selected-table"], 'updated_data.csv.gz', is_incremental)
            st.success('Data Updated!', icon="🎉")

# Data editor
edited_data = st.data_editor(st.session_state["data"], num_rows="dynamic", height=500, use_container_width=True)

if st.button("Save Data", key="save-data-tables"):
    with st.spinner('Saving Data...'):
        kbc_data = cast_bool_columns(get_dataframe(st.session_state["selected-table"]))
        edited_data = cast_bool_columns(edited_data)
        st.session_state["data"] = edited_data
        concatenated_df = pd.concat([kbc_data, edited_data])
        sym_diff_df = concatenated_df.drop_duplicates(keep=False)
        is_incremental = False  # Change to True if primary keys are set
        write_to_keboola(edited_data, st.session_state["selected-table"], f'updated_data.csv.gz', is_incremental)
    st.success('Data Updated!', icon="🎉")

display_footer_section()
