import streamlit as st
import pandas as pd
from utils.db import run_query, MARTS_SCHEMA
from utils.charts import salary_comparison_bar

st.set_page_config(page_title='Market Benchmarks', layout='wide')
st.title('🌍 Market Benchmarks')
st.caption('Salary comparison and posting volume across Kenya, UK, and US')

# Data loading

@st.cache_data(ttl=3600, show_spinner='Loading benchmark data...')
def load_benchmarks():
    return run_query(
        f'''
            select
                title_normalized,
                role_family,
                market,
                country_clean,
                is_management,
                is_technical,
                total_postings,
                postings_with_salary,
                salary_disclosure_rate_pct,
                salary_currency,
                salary_median_annual,
                salary_min_annual,
                salary_max_annual,
                base_salary_median_annual,
                additional_pay_pct,
                salary_bucket,
                base_salary_bucket,
                estimate_reliability,
                estimate_sample_size,
                salary_source
            from {MARTS_SCHEMA}.mart_job_market_benchmarks
        '''
    )
df = load_benchmarks()

if df.empty:
    st.warning('No data returned from mart_job_market_benchmarks.')
    st.stop()

# Sidebar filters

with st.sidebar:
    st.header('Filters')
    
    selected_markets = st.multiselect(
        'Market',
        options=sorted(df['market'].unique()),
        default=sorted(df['market'].unique())
    )
    
    selected_families = st.multiselect(
        'Role Family',
        options=sorted(df['role_family'].dropna().unique()),
        default=sorted(df['role_family'].dropna().unique())
    )
    
    reliability_filter = st.multiselect(
        'Estimate reliability',
        options=['high', 'medium', 'low'],
        default=['high', 'medium']
    )
# Apply filters

filtered = df[
    df['market'].isin(selected_markets) &
    df['role_family'].isin(selected_families) #&
    #df['estimate_reliability'].isin(reliability_filter)
].copy()

if filtered.empty:
    st.warning('No data matches the selected filters.')
    st.stop()

# KPI row

st.subheader('Summary')
k1, k2, k3, k4 = st.columns(4)

k1.metric(
    'Total Roles',
    f"{filtered['title_normalized'].nunique():,}"
)
k2.metric(
    'Total Postings',
    f"{filtered['total_postings'].sum():,}"
)
k3.metric(
    'Avg Disclosure Rate',
    f"{filtered['salary_disclosure_rate_pct'].mean():.1f}%"
)
k4.metric(
    'Markets',
    filtered['market'].nunique()
)

st.divider()

# Tab layout

tab1, tab2, tab3, tab4 = st.tabs([
    '💰 Salary Comparison',
    '📊 Posting Volume',
    '🔍 Disclosure Rate',
    '📋 Data Table'
])