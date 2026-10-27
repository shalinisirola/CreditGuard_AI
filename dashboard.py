import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go

# Page Config
st.set_page_config(
    page_title="CreditGuard AI Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main {background-color: #0e1117;}
    .stMetric {background-color: #262730; padding: 15px; border-radius: 10px;}
    h1 {color: #00d4ff;}
    .fraud-alert {
        background-color: #ff4b4b;
        color: white;
        padding: 10px;
        border-radius: 5px;
        animation: pulse 1s infinite;
    }
    @keyframes pulse {
        0%, 100% {opacity: 1;}
        50% {opacity: 0.5;}
    }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown("# 🛡️ CreditGuard AI: Real-Time Fraud Detection")
st.markdown("### Live Transaction Monitoring System")

# Initialize Session State
if 'data' not in st.session_state:
    st.session_state.data = []
if 'fraud_count' not in st.session_state:
    st.session_state.fraud_count = 0
if 'total_count' not in st.session_state:
    st.session_state.total_count = 0
if 'location_fraud' not in st.session_state:
    st.session_state.location_fraud = {}

# Kafka Consumer
@st.cache_resource
def get_consumer():
    try:
        return KafkaConsumer(
            'all_predictions',
            bootstrap_servers=['kafka:29092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='dashboard_monitor',
            consumer_timeout_ms=500
        )
    except Exception as e:
        st.error(f"Kafka Connection Error: {e}")
        return None

consumer = get_consumer()

# Consume new messages
if consumer:
    messages = consumer.poll(timeout_ms=500)
    if messages:
        for tp, msgs in messages.items():
            for msg in msgs:
                record = msg.value
                st.session_state.total_count += 1
                
                fraud_score = float(record.get('fraud_score', 0.0))
                is_fraud = bool(record.get('review_required', fraud_score >= 0.7))
                if is_fraud:
                    st.session_state.fraud_count += 1
                    location = record.get('location', 'Unknown')
                    st.session_state.location_fraud[location] = st.session_state.location_fraud.get(location, 0) + 1
                
                st.session_state.data.append({
                    'timestamp': pd.Timestamp.now(),
                    'amount': record.get('Amount', record.get('amount', 0)),
                    'location': record.get('location', record.get('source', 'Unknown')),
                    'is_fraud': is_fraud,
                    'fraud_score': fraud_score,
                    'latency_ms': record.get('latency_ms'),
                    'user_id': record.get('user_id', 'Unknown')
                })
        
        st.session_state.data = st.session_state.data[-100:]

# === METRICS ROW ===
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("📊 Total Transactions", st.session_state.total_count)
with col2:
    st.metric("🚨 Fraud Detected", st.session_state.fraud_count, delta_color="inverse")
with col3:
    fraud_rate = 0
    if st.session_state.total_count > 0:
        fraud_rate = (st.session_state.fraud_count / st.session_state.total_count) * 100
    st.metric("📈 Fraud Rate", f"{fraud_rate:.1f}%")

st.divider()

# === CHARTS ROW ===
if st.session_state.data:
    df = pd.DataFrame(st.session_state.data)
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("📊 Fraud vs Normal Distribution")
        fraud_counts = df['is_fraud'].value_counts()
        pie_data = pd.DataFrame({
            'Status': ['Normal' if idx == False else 'Fraud' for idx in fraud_counts.index],
            'Count': fraud_counts.values
        })
        fig_pie = px.pie(pie_data, values='Count', names='Status', 
                         color='Status',
                         color_discrete_map={'Normal': '#00d4ff', 'Fraud': '#ff4b4b'})
        fig_pie.update_layout(height=300, margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with chart_col2:
        st.subheader("💰 Transaction Amount Over Time")
        df_sorted = df.sort_values('timestamp')
        fig_line = px.line(df_sorted, x='timestamp', y='amount', 
                           color='is_fraud',
                           color_discrete_map={True: '#ff4b4b', False: '#00d4ff'})
        fig_line.update_layout(height=300, margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig_line, use_container_width=True)

st.divider()

# === FRAUD BY LOCATION ===
chart_col3, table_col = st.columns([1, 2])

with chart_col3:
    st.subheader("🗺️ Fraud by Location")
    if st.session_state.location_fraud:
        loc_df = pd.DataFrame(list(st.session_state.location_fraud.items()), columns=['Location', 'Frauds'])
        loc_df = loc_df.sort_values('Frauds', ascending=True)
        fig_bar = px.bar(loc_df, x='Frauds', y='Location', orientation='h',
                         color='Frauds', color_continuous_scale='Reds')
        fig_bar.update_layout(height=300, margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No fraud detected yet")

with table_col:
    st.subheader("📋 Recent Transactions")
    if st.session_state.data:
        display_df = df[['timestamp', 'user_id', 'amount', 'location', 'fraud_score', 'latency_ms', 'is_fraud']].tail(10)
        display_df['is_fraud'] = display_df['is_fraud'].map({True: '🚨 FRAUD', False: '✅ Normal'})
        display_df['timestamp'] = display_df['timestamp'].dt.strftime('%H:%M:%S')
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("Waiting for transactions...")

# === STATUS FOOTER ===
st.divider()
status_col1, status_col2 = st.columns(2)
with status_col1:
    st.success("✅ System Running")
with status_col2:
    st.caption(f"🕒 Last Update: {pd.Timestamp.now().strftime('%H:%M:%S')} | Auto-refresh: 3s")

# Auto-refresh
time.sleep(3)
st.rerun()
