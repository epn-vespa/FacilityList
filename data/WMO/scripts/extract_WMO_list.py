import pandas as pd
import json

df = pd.read_excel (r"/Users/ldebisschop/Documents/GitHub/FacilityList/data/WMO/WMO_list.xlsx")
print (df)

with open("WMO_list.json", "w") as f:
    f.write(json.dumps(df, indent=4))


