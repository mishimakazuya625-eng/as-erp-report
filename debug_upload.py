import pandas as pd

# Simulate the upload process
file_path = r'd:\vs\ASSYSTEM\In-transit.csv'

# Try to read with different encodings
try:
    df = pd.read_csv(file_path, encoding='cp949')
except:
    df = pd.read_csv(file_path, encoding='utf-8-sig')

print("=== CSV Columns (Raw) ===")
for i, col in enumerate(df.columns):
    print(f"  [{i}] '{col}' (repr: {repr(col)})")

REQUIRED_LOCATIONS = ['114(A/S창고)', '114C(천안 A/S창고)', '114R(부산 A/S창고)', '111H(HMC창고)', '운송중(927SF)', '운송중(111S)', '운송중(DEY)']

def normalize_str(s):
    return "".join(str(s).split()).upper()

print("\n=== REQUIRED_LOCATIONS (Normalized) ===")
norm_required = {normalize_str(loc): loc for loc in REQUIRED_LOCATIONS}
for norm, orig in norm_required.items():
    print(f"  '{norm}' -> '{orig}'")

print("\n=== Matching Process ===")
present_locations = []
for col in df.columns:
    norm_col = normalize_str(col)
    if norm_col in norm_required:
        df.rename(columns={col: norm_required[norm_col]}, inplace=True)
        present_locations.append(norm_required[norm_col])
        print(f"  MATCH: '{col}' -> '{norm_required[norm_col]}'")
    else:
        print(f"  NO MATCH: '{col}' (normalized: '{norm_col}')")

print(f"\n=== Present Locations ({len(present_locations)}) ===")
print(present_locations)

# Show 114R data
if '114R(부산 A/S창고)' in df.columns:
    print("\n=== 114R Data Sample (non-zero) ===")
    non_zero = df[df['114R(부산 A/S창고)'] > 0][['PN', '114R(부산 A/S창고)']].head(10)
    print(non_zero)
else:
    print("\n=== 114R column NOT FOUND in df.columns ===")
    print("Current columns:", df.columns.tolist())
