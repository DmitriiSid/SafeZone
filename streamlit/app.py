import streamlit as st
import pandas as pd

# Load the DataFrame
df = pd.read_csv('db_pomoci_streamlit.csv')

# Function to add a new contact
def add_contact(df):
    with st.form("add_contact_form"):
        nazev = st.text_input("Název")
        E_mail = st.text_input("E_mail")
        Telefon = st.text_input("Telefon")
        address = st.text_input("Address")
        submitted = st.form_submit_button("Add Contact")
        if submitted:
            new_contact = {"Název": nazev, "E_mail": E_mail, "Telefon": Telefon, "Address": address}
            #df = df.append(new_contact, ignore_index=True)
            st.success("Contact added successfully!")
            #df.to_csv('db_pomoci_streamlit.csv', index=False)
    return df

# Function to filter and update a contact
def update_contact(df):
    contact_to_update = st.selectbox("Select Contact to Update", df['Název'].unique())
    if contact_to_update:
        contact_info = df[df['Název'] == contact_to_update].iloc[0]
        with st.form("update_contact_form"):
            nazev = st.text_input("Název", value=contact_info['Název'])
            E_mail = st.text_input("E_mail", value=contact_info['E_mail'])
            Telefon = st.text_input("Telefon", value=contact_info['Telefon'])
            address = st.text_input("Address", value=contact_info['Address'])
            submitted = st.form_submit_button("Update Contact")
            if submitted:
                df.loc[df['Název'] == contact_to_update, ['Název', 'E_mail', 'Telefon', 'Address']] = [nazev, E_mail, Telefon, address]
                st.success("Contact updated successfully!")
                df.to_csv('db_pomoci_streamlit.csv', index=False)
    return df

# Function to delete a contact
def delete_contact(df):
    contact_to_delete = st.selectbox("Select Contact to Delete", df['Název'].unique())
    if st.button("Delete Contact"):
        df = df[df['Název'] != contact_to_delete]
        st.success("Contact deleted successfully!")
        df.to_csv('db_pomoci_streamlit.csv', index=False)
    return df

# Streamlit app
st.title("Contact Management Application")

# Add new contact
st.header("Add New Contact")
df = add_contact(df)

# Update existing contact
# st.header("Update Contact")
# df = update_contact(df)

# # Delete contact
# st.header("Delete Contact")
# df = delete_contact(df)

# Display the DataFrame
#st.header("All Contacts")
with st.expander("All Contacts"):
    st.dataframe(df)
