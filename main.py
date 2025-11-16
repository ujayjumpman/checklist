import streamlit as st


pages = {
    "Projects": [
        st.Page("eden.py", title="EDEN"),
        st.Page("checklistews.py", title="EWS Checklist"),
        st.Page("Wave City.py", title="Wave City Checklist"),
        st.Page("CheckEligo.py", title="Eligo Checklist"),
        st.Page("veridia.py", title="Veridia"),
    ]
}


pg = st.navigation(pages)

pg.run()

