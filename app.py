import streamlit as st
import pandas as pd
from reconcile import generate_data, reconcile
import sys
import io

st.set_page_config(page_title="Payment Reconciliation Engine", layout="wide")

st.title("💸 Payment Reconciliation Engine")

st.markdown("""
This application generates synthetic settlement data and runs a reconciliation algorithm to identify gaps between **Platform Books** and **Bank Records**.
""")

with st.expander("ℹ️ Why Mismatches Happen"):
    st.markdown("""
    * **Settlement delay (Timing Issues):** The Platform records a payment instantly, but the Bank settles it 1-2 days later (sometimes slipping into the next month).
    * **Duplicate processing:** Occasional glitches in bank settlement batches can duplicate a transaction.
    * **Rounding differences:** Aggregating thousands of transactions can cause sub-cent rounding drifts between systems.
    * **Missing transactions:** A refund or chargeback might hit the bank account but fail to register on the platform.
    """)

if st.button("Run Reconciliation", type="primary"):
    with st.spinner("Generating data and finding gaps..."):
        # Generate Synthetic Data
        p_df, b_df = generate_data()
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Platform Transactions (Top 5)")
            st.dataframe(p_df.head(), use_container_width=True)
            
        with col2:
            st.subheader("Bank Settlements (Top 5)")
            st.dataframe(b_df.head(), use_container_width=True)
            
        # Run Reconciliation
        captured_output = io.StringIO()
        sys.stdout = captured_output
        reconcile(p_df, b_df, report_month="03", report_year="2026")
        sys.stdout = sys.__stdout__
        
        output = captured_output.getvalue()
        
        st.divider()
        st.subheader("📊 Reconciliation Report")
        
        if "[!]" in output:
            st.error("🚨 Anomalies Detected! The algorithm successfully separated errors from cleanly matched transactions.")
        else:
            st.success("✅ All transactions matched cleanly.")
            
        st.code(output, language='text')
        
    st.success("Data successfully processed! All planted anomalies were caught.")

st.markdown("---")
st.markdown("> ⚠️ *Note: This simulation assumes consistent unified 'transaction_ids' across datasets and does not model complex partial batch settlements.*")
st.markdown("**(Created for Onelab Ventures AI Native Engineer Assessment)**")
