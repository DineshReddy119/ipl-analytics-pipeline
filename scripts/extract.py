import pandas as pd

def extract_matches():
    df = pd.read_csv("data/matches.csv")
    print(f"✅ Matches loaded: {len(df)} rows")
    print(f"   Columns: {list(df.columns)}")
    return df

def extract_deliveries():
    df = pd.read_csv("data/deliveries.csv")
    print(f"✅ Deliveries loaded: {len(df)} rows")
    print(f"   Columns: {list(df.columns)}")
    return df

if __name__ == "__main__":
    matches = extract_matches()
    deliveries = extract_deliveries()