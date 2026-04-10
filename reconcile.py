import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_data():
    np.random.seed(42)
    start_date = datetime(2026, 3, 1)
    
    platform_data = []
    bank_data = []
    
    # Generate 50 normal transactions
    for i in range(1, 51):
        txn_id = f"TXN_{i:04d}"
        days_offset = np.random.randint(0, 25)
        txn_date = start_date + timedelta(days=days_offset)
        amount = round(np.random.uniform(10.0, 500.0), 2)
        
        platform_data.append({
            "transaction_id": txn_id,
            "date": txn_date.strftime("%Y-%m-%d"),
            "amount": amount,
            "type": "payment"
        })
        
        bank_data.append({
            "transaction_id": txn_id,
            "date": (txn_date + timedelta(days=2)).strftime("%Y-%m-%d"),
            "amount": amount,
            "type": "payment"
        })
        
    platform_df = pd.DataFrame(platform_data)
    bank_df = pd.DataFrame(bank_data)
    
    # -- Introduce Anomalies --
    
    # 1. A transaction settled in the next month
    # We will modify the last transaction so platform is March 31, Bank is April 2
    platform_df.at[49, "date"] = "2026-03-31"
    bank_df.at[49, "date"] = "2026-04-02"
    
    # 2. A duplicate entry in the settlement dataset
    dup_row = bank_df.iloc[[5]].copy()
    bank_df = pd.concat([bank_df, dup_row], ignore_index=True)
    
    # 3. A rounding difference that appears only when aggregated
    # We'll slightly alter one bank transaction amount by 0.01 to represent a rounding issue.
    # At scale, 0.01 diffs can be caused by tax rounding differently on aggregate vs line-item.
    bank_df.at[10, "amount"] = round(bank_df.at[10, "amount"] - 0.01, 2)
    
    # 4. A refund entry with no corresponding original transaction
    bank_df = pd.concat([bank_df, pd.DataFrame([{
        "transaction_id": "REF_9999",
        "date": "2026-03-15",
        "amount": -50.00,
        "type": "refund"
    }])], ignore_index=True)
    
    return platform_df, bank_df

def reconcile(platform_df, bank_df, report_month="03", report_year="2026"):
    print(f"--- Reconciliation Report for {report_year}-{report_month} ---")
    
    # Filter datasets for the current month logically
    # Platform records what was initiated this month
    platform_month = platform_df[platform_df['date'].str.startswith(f"{report_year}-{report_month}")]
    
    # Bank records what settled this month
    bank_month = bank_df[bank_df['date'].str.startswith(f"{report_year}-{report_month}")]
    
    # 1. Check for duplicates in bank settlements
    bank_duplicates = bank_month[bank_month.duplicated(subset=['transaction_id', 'amount', 'date'], keep=False)]
    if not bank_duplicates.empty:
        print("\n[!] ANOMALY FOUND: Duplicate entries in Settlement Dataset")
        print(bank_duplicates.sort_values(by='transaction_id').to_string(index=False))
        # Remove duplicates for further reconciliation to avoid noise
        bank_month = bank_month.drop_duplicates(subset=['transaction_id', 'amount', 'date'], keep='first')
        bank_df = bank_df.drop_duplicates(subset=['transaction_id', 'amount', 'date'], keep='first')
        
    # 2. Reconcile by linking transaction IDs
    merged = pd.merge(platform_month, bank_month, on="transaction_id", how="outer", suffixes=('_plat', '_bank'))
    
    # 3. Missing in Bank (Settlement in next month or missing entirely)
    missing_in_bank = merged[merged['amount_bank'].isna()]
    if not missing_in_bank.empty:
        print("\n[!] ANOMALY FOUND: Transactions on Platform not settled in Bank in current month")
        # Check if they settled in the future (the full bank_df)
        for _, row in missing_in_bank.iterrows():
            future_settle = bank_df[bank_df['transaction_id'] == row['transaction_id']]
            if not future_settle.empty:
                print(f"  - Timing Issue: {row['transaction_id']} (Platform: {row['date_plat']}) settled later on {future_settle.iloc[0]['date']}.")
            else:
                print(f"  - Missing: {row['transaction_id']} never settled.")
                
    # 4. Unknown Settlements / Refunds missing on platform
    missing_in_platform = merged[merged['amount_plat'].isna()]
    if not missing_in_platform.empty:
        print("\n[!] ANOMALY FOUND: Bank settlements with no matching Platform transaction")
        print(missing_in_platform[['transaction_id', 'date_bank', 'amount_bank', 'type_bank']].to_string(index=False))
        
    # 5. Amount Discrepancies (Rounding)
    # Find rows where both exist but amounts differ
    both_exist = merged.dropna(subset=['amount_plat', 'amount_bank'])
    amount_diffs = both_exist[both_exist['amount_plat'] != both_exist['amount_bank']].copy()
    if not amount_diffs.empty:
        print("\n[!] ANOMALY FOUND: Amount discrepancies between Platform and Bank (Rounding)")
        amount_diffs['difference'] = round(amount_diffs['amount_plat'] - amount_diffs['amount_bank'], 2)
        print(amount_diffs[['transaction_id', 'amount_plat', 'amount_bank', 'difference']].to_string(index=False))
        
    # 6. Aggregate Check
    total_plat = platform_month['amount'].sum()
    total_bank = bank_month['amount'].sum()
    print(f"\n--- Aggregated Totals ---")
    print(f"Platform Total: ${total_plat:,.2f}")
    print(f"Bank Total:     ${total_bank:,.2f}")
    
    # The pure aggregation difference is total_plat vs total_bank, explaining the gap.
    gap = total_plat - total_bank
    print(f"Net Gap for Month: ${gap:,.2f}")

if __name__ == "__main__":
    p_df, b_df = generate_data()
    # Save datasets locally just in case
    p_df.to_csv("platform_transactions.csv", index=False)
    b_df.to_csv("bank_settlements.csv", index=False)
    print("Synthetic datasets generated and saved as CSV.")
    
    reconcile(p_df, b_df, report_month="03", report_year="2026")
