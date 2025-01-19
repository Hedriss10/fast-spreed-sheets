import pandas as pd

data1 = {
    "phone": ["123456789", "987654321", "555555555", "777777777"],
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com"],
}

data2 = {
    "phone": ["123456789", "555555555", "999999999", "888888888"],
    "address": ["123 Main St", "555 Elm St", "999 Pine St", "888 Oak St"],
    "city": ["New York", "Los Angeles", "Chicago", "Houston"],
}

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

df1.to_csv("df1.csv", index=False)
df2.to_csv("df2.csv", index=False)