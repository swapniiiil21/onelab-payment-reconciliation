import pytest
import pandas as pd
from reconcile import reconcile, generate_data
import io
import sys

def test_generate_data_has_anomalies():
    p_df, b_df = generate_data()
    
    # 1. Check for timing anomaly: transaction settled in next month
    assert p_df.at[49, "date"] == "2026-03-31"
    assert b_df.at[49, "date"] == "2026-04-02"
    
    # 2. Check for duplicate entry
    # 50 base transactions + 1 duplicate + 1 refund = 52
    assert len(p_df) == 50
    assert len(b_df) == 52
    
    # 3. Check for rounding difference on TXN_0011 (index 10)
    assert round(p_df.at[10, "amount"] - b_df.at[10, "amount"], 2) == 0.01

def test_reconciliation_output():
    p_df, b_df = generate_data()
    
    # Capture standard output
    captured_output = io.StringIO()
    sys.stdout = captured_output
    reconcile(p_df, b_df, report_month="03", report_year="2026")
    sys.stdout = sys.__stdout__
    
    output = captured_output.getvalue()
    
    # Verify the script caught all 4 planted gaps
    assert "Duplicate entries in Settlement Dataset" in output
    assert "Transactions on Platform not settled in Bank in current month" in output
    assert "Bank settlements with no matching Platform transaction" in output
    assert "Amount discrepancies between Platform and Bank (Rounding)" in output

if __name__ == "__main__":
    pytest.main(["-v", __file__])
