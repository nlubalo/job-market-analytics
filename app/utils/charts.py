# utils/charts.py
# =============================================================
# Reusable chart functions using Plotly
# All charts return a plotly Figure object — rendered in pages
# via st.plotly_chart(fig, use_container_width=True)
# =============================================================

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Consistent market colours across all dashboards
MARKET_COLORS = {
    'Kenya': '#1D9E75',     # teal
    'UK':    '#534AB7',     # purple
    'US':    '#D85A30',     # coral
    'Other': '#888780',     # gray
}

SKILL_CATEGORY_COLORS = {
    'Data Stack': '#534AB7',
    'Cloud':      '#1D9E75',
    'AI/ML':      '#D85A30',
}

def salary_comparison_bar(
    df: pd.DataFrame,
    salary_col: str = 'slalry_median_annual',
    title: str = 'Salary Comparison by Role and Market'
    ) -> go.Figure:
    
    fig = px.bar(
        df,
        x='title_normalized',
        color='market',
        barmode='group',
        color_discrete_map=MARKET_COLORS,
        labels={
            salary_col: 'Annual Salary (local currency)',
            'title_normalized': 'Role',
            'market': 'Market'
        },
        title=title,
        hover_data=['salary_currency', 'estimate_reliability', 'estimate_sample_size']
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        legend_title='Market',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(size=12),
        height=500
    )
    return fig