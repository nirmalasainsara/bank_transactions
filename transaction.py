import pdfplumber
import sqlite3

# Part 1: Data Extraction
def extract_transaction_details(pdf_path):
    transactions = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')
            for line in lines:
                fields = line.split()
                if len(fields) == 11:
                    transaction = {
                        "app_id": fields[0],
                        "xref": fields[1],
                        "settlement_date": fields[2],
                        "broker": fields[3],
                        "sub_broker": fields[4],
                        "borrower_name": " ".join(fields[5:-6]),
                        "description": fields[-6],
                        "total_loan_amount": float(fields[-5].replace(",", "")),
                        "commission_rate": float(fields[-4]),
                        "upfront": float(fields[-3].replace(",", "")),
                        "upfront_incl_gst": float(fields[-2].replace(",", ""))
                    }
                    transactions.append(transaction)
    return transactions

# Part 2: Data Storage
def create_database():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS transactions (
                 app_id TEXT,
                 xref TEXT,
                 settlement_date TEXT,
                 broker TEXT,
                 sub_broker TEXT,
                 borrower_name TEXT,
                 description TEXT,
                 total_loan_amount REAL,
                 commission_rate REAL,
                 upfront REAL,
                 upfront_incl_gst REAL
                 )''')
    conn.commit()
    conn.close()

def insert_into_database(transactions):
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()
    for transaction in transactions:
        c.execute('''INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                  (transaction['app_id'],
                   transaction['xref'],
                   transaction['settlement_date'],
                   transaction['broker'],
                   transaction['sub_broker'],
                   transaction['borrower_name'],
                   transaction['description'],
                   transaction['total_loan_amount'],
                   transaction['commission_rate'],
                   transaction['upfront'],
                   transaction['upfront_incl_gst']))
    conn.commit()
    conn.close()

# Part 3: Deduplication
def remove_duplicates():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()
    c.execute('''DELETE FROM transactions WHERE rowid NOT IN (
                 SELECT MIN(rowid) FROM transactions GROUP BY xref, total_loan_amount)''')
    conn.commit()
    conn.close()

# Part 4: SQL Operations
def total_loan_amount_by_period(start_date, end_date):
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()
    c.execute('''SELECT SUM(total_loan_amount) FROM transactions WHERE settlement_date BETWEEN ? AND ?''',
              (start_date, end_date))
    total_amount = c.fetchone()[0]
    conn.close()
    return total_amount

def highest_loan_amount_by_broker():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()
    c.execute('''SELECT broker, MAX(total_loan_amount) FROM transactions GROUP BY broker''')
    result = c.fetchall()
    conn.close()
    return result

# Part 5: Reporting

def generate_report_sorted_loan_amount(period):
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    # Selecting total loan amounts grouped by borrower name
    if period == "daily":
        c.execute('''SELECT borrower_name, SUM(total_loan_amount) AS total_amount 
                     FROM transactions GROUP BY borrower_name''')
    elif period == "weekly":
        c.execute('''SELECT strftime('%W', settlement_date) AS week_number, 
                            SUM(total_loan_amount) AS total_amount 
                     FROM transactions GROUP BY week_number''')
    elif period == "monthly":
        c.execute('''SELECT strftime('%m-%Y', settlement_date) AS month_year, 
                            SUM(total_loan_amount) AS total_amount 
                     FROM transactions GROUP BY month_year''')

    report = c.fetchall()
    conn.close()
    return report

def generate_report_total_loan_amount_by_date():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()
    c.execute('''SELECT settlement_date, SUM(total_loan_amount) AS total_amount 
                 FROM transactions GROUP BY settlement_date''')
    report = c.fetchall()
    conn.close()
    return report

def generate_tier_level_report():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()
    c.execute('''SELECT settlement_date,
                         CASE 
                            WHEN total_loan_amount > 100000 THEN 'Tier 1'
                            WHEN total_loan_amount > 50000 THEN 'Tier 2'
                            WHEN total_loan_amount > 10000 THEN 'Tier 3'
                            ELSE 'Other'
                         END AS tier_level, 
                         COUNT(*) AS count 
                 FROM transactions GROUP BY settlement_date, tier_level''')
    report = c.fetchall()
    conn.close()
    return report


if __name__ == "__main__":
    pdf_path = "/home/nirmala/Downloads/test_pdf.pdf"
    transactions = extract_transaction_details(pdf_path)
    create_database()
    insert_into_database(transactions)
    remove_duplicates()
    print("Data extraction, storage, and deduplication complete.")