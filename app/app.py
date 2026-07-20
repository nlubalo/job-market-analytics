# Main entry point
# Sets page config and renders the sidebar navigation

import streamlit as st
from utils.db import clear_cache

st.set_page_config(
    page_title ='Job Market Analytics',
    page_icon ='📊',
    layout='wide',
    initial_sidebar_state='expanded'
)

# Global sidebar

with st.sidebar:
    st.title('📊 Job Market Analytics')
    st.caption('Kenya · UK · US tech job market benchmarking')
    st.divider()
    
    st.markdown('''
        **Dashboards**
        - 🌍 Market Benchmarks
        - 📈 Salary Trends
        - 🛠 Skill Demand
        - 🏢 Company Hiring
        ''')
    
    st.divider()
    
    st.divider()

    if st.button('🔄 Refresh Data', use_container_width=True):
        clear_cache()
        st.success('Cache cleared — data will reload on next query.')

    st.caption('Data refreshes automatically every hour.')
    st.caption('Built on JSearch API + dbt + Databricks')
    
st.title('Job Market Analytics Platform')
st.markdown('''
Benchmarking data and tech roles across **Kenya**, **UK**, and **US**
using job posting and salary estimate data from JSearch.

Use the sidebar to navigate between dashboards.
''')

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.info('🌍 **Market Benchmarks**\nSalary comparison and posting volume by role and market')
with col2:
    st.info('📈 **Salary Trends**\nHow salaries are moving over time')
with col3:
    st.info('🛠 **Skill Demand**\nWhich skills are most in demand and where')
with col4:
    st.info('🏢 **Company Hiring**\nTop hiring companies by market and role')