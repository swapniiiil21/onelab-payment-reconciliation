import streamlit as st
import pandas as pd
from reconcile import generate_data, reconcile
import sys
import io

st.set_page_config(page_title="Payment Reconciliation", layout="wide")

st.title("Payment Reconciliation Engine")

st.markdown("""
This application generates synthetic settlement data and runs a reconciliation algorithm to identify gaps between Platform books and Bank records.
""")

if st.button("Run Reconciliation", type="primary"):
    with st.spinner("Generating data and finding gaps..."):
        # 1. Generate Synthetic Data
        p_df, b_df = generate_data()
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Platform Transactions (Top 5)")
            st.dataframe(p_df.head(), use_container_width=True)
            
        with col2:
            st.subheader("Bank Settlements (Top 5)")
            st.dataframe(b_df.head(), use_container_width=True)
            
        # 2. Run Reconciliation
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        reconcile(p_df, b_df, report_month="03", report_year="2026")
        
        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()
        
        # 3. Display Results
        st.subheader("Reconciliation Report")
        
        # We can parse the output a bit for better display, or just show as code block
        if "[!]" in output:
            st.error("Anomalies Detected!")
        else:
            st.success("All transactions matched cleanly.")
            
        st.code(output, language='text')

st.markdown("---")
st.markdown("**(Created for Onelab Ventures AI Native Engineer Assessment)**")
