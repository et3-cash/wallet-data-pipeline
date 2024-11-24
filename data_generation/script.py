import csv
import json
import random
from faker import Faker

fake = Faker()

# Constants
NUM_RECORDS = 10000

# Generate Users CSV
def generate_users_csv(filename):
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["user_id", "phone_number", "joined_date", "last_login", "is_active", "is_admin"])
        
        for i in range(1, NUM_RECORDS + 1):
            writer.writerow([
                i,
                fake.phone_number(),
                fake.date_time_this_decade().isoformat(),
                fake.date_time_this_year().strftime("%Y/%m/%d %H:%M:%S"),
                random.choice(["true", "TRUE", "false", "FALSE"]),
                random.choice(["true", "TRUE", "false", "FALSE"])
            ])

# Generate Accounts JSON
def generate_accounts_json(filename):
    data = []
    for i in range(1, NUM_RECORDS + 1):
        data.append({
            "account_id": i,
            "balance": random.choice([f"{random.uniform(-1000, 5000):.2f}", "negative 50.25", "error"]),
            "user_id": i
        })
    
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)

# Generate Transactions CSV
def generate_transactions_csv(filename):
    transaction_types = ["Deposit", "Withdrawal", "TRANSFER"]
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["transaction_id", "transaction_type", "amount", "created_at", "description", "user_id"])
        
        for i in range(1, NUM_RECORDS + 1):
            writer.writerow([
                i,
                random.choice(transaction_types),
                f"{random.uniform(-500, 1000):.2f}",
                fake.date_time_this_year().strftime("%d-%m-%Y %H:%M:%S"),
                fake.sentence(nb_words=3),
                random.randint(1, NUM_RECORDS)
            ])

# Generate Files
generate_users_csv("users.csv")
generate_accounts_json("accounts.json")
generate_transactions_csv("transactions.csv")
