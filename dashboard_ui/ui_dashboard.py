import os
import sys

# --- FORCE HIGH-PRIORITY PATH OVERRIDES FIRST ---
# 1. Inject virtual environment paths explicitly to guarantee package visibility
VENV_PACKAGES = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "myenv", "Lib", "site-packages"))
if os.path.exists(VENV_PACKAGES) and VENV_PACKAGES not in sys.path:
    sys.path.insert(0, VENV_PACKAGES)

# 2. Inject your Django project folder path
DJANGO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "my_django_project"))
if DJANGO_ROOT not in sys.path:
    sys.path.insert(0, DJANGO_ROOT)

# 3. Bootstrap the independent Django configuration settings context
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_django_project.settings")
import django
django.setup()

# --- NOW SAFELY IMPORT REMAINING USER INTERFACE FRAMEWORKS ---
import pandas as pd
import streamlit as st
import plotly.express as px
from intelligence_app.models import MarketIntelligenceReport

# --- Configure Page Properties ---
st.set_page_config(
    page_title="AI Market Intelligence Console",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Theme Application Styling Customizations ---
st.markdown("""
    <style>
    .main .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    h1 { color: #0E1117; font-weight: 800; }
    .stMetric { background-color: #F0F2F6; padding: 15px; border-radius: 10px; border-left: 5px solid #ff4b4b; }
    </style>
""", unsafe_allow_html=True)

st.title("📊 Autonomous Market Intelligence Console")
st.caption("Live operational dashboard displaying real-time multi-agent loops, database rows, and telemetry analytics.")

# --- Data Extraction Layer ---
@st.cache_data(ttl=10) # Cache data for 10 seconds to optimize performance while allowing real-time syncs
def load_pipeline_data_from_db():
    """Queries your database tables natively via Django ORM and returns a parsed DataFrame."""
    queryset = MarketIntelligenceReport.objects.all().values()
    df = pd.DataFrame(list(queryset))
    if not df.empty:
        df['created_at'] = pd.to_datetime(df['created_at'])
    return df

df_reports = load_pipeline_data_from_db()

# --- Main Workspace Rendering Blocks ---
if df_reports.empty:
    st.warning("📥 Operational Alert: Waiting for stream ingestion data packets... Ensure your backend service is running.")
    if st.button("🔄 Check for Fresh Inflow Records"):
        st.rerun()
else:
    # 1. Summary Analytics Metrics Panel Grid
    total_records = len(df_reports)
    avg_revisions = float(df_reports['validation_loops_count'].mean())
    latest_sync_time = df_reports['created_at'].max().strftime('%H:%M:%S')

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric(label="Total Telemetry Rows Saved", value=total_records)
    with m2:
        st.metric(label="Average Agentic Validation Loops", value=f"{avg_revisions:.1f} Runs")
    with m3:
        st.metric(label="Last Database Sync Time", value=latest_sync_time)

    st.markdown("---")

    # 2. Main View Grid Layout Split: Left for Agent Reports, Right for Charts Analytics
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("📰 Generated Gemini Executive Reports")
        
        # Build dropdown selection array using database IDs and timestamps
        report_selector_map = {
            f"ID Reference: {row['id']} | Feed: {row['source_feed']} ({row['created_at'].strftime('%H:%M:%S')})": row['id']
            for _, row in df_reports.iterrows()
        }
        
        selected_report_label = st.selectbox("Choose a record row from the historical stream queue:", list(report_selector_map.keys()))
        selected_db_id = report_selector_map[selected_report_label]
        
        # Filter selected object row parameters
        selected_record = df_reports[df_reports['id'] == selected_db_id].iloc[0]
        
        # Display underlying data metrics summary
        with st.expander("🔍 View Raw Ingestion Text Received via Kafka Queue Stream"):
            st.code(selected_record['original_alert_text'], language="text")
            
        st.markdown("#### Final Curated Report Document (Rendered Markdown Layout):")
        st.markdown(selected_record['generated_markdown_report'])

    with col_right:
        st.subheader("⚙️ System Pipeline Telemetry")
        
        # Chart A: Bar layout visualization tracing agent execution loops over historical sequences
        st.write("**Agentic Re-run Cycle Counts per Row Instance:**")
        fig_loops = px.bar(
            df_reports,
            x='id',
            y='validation_loops_count',
            color='status',
            color_discrete_map={'Approved': '#00CC96', 'RevisionNeeded': '#EF553B'},
            labels={'id': 'Database Row ID', 'validation_loops_count': 'Validation Iteration Count'},
            template="plotly_white"
        )
        fig_loops.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=300)
        st.plotly_chart(fig_loops, use_container_width=True)
        
        # Chart B: Distribution breakdown chart displaying sources of incoming metrics feeds
        st.write("**Data Inflow Source Mix Percentage Breakdown:**")
        source_mix = df_reports['source_feed'].value_counts().reset_index()
        source_mix.columns = ['Source Name', 'Packet Count']
        
        fig_pie = px.pie(
            source_mix,
            values='Packet Count',
            names='Source Name',
            hole=0.4,
            template="plotly_white"
        )
        fig_pie.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=250)
        st.plotly_chart(fig_pie, use_container_width=True)

        # Dashboard refresh trigger hook
        if st.button("🔄 Sync Interface Workspace Data State"):
            st.cache_data.clear()
            st.rerun()
