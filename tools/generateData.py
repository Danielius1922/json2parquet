import random
import base64
import string
import json
import nltk
from nltk.corpus import words

# Download NLTK words if not already downloaded
nltk.download('words')

# Get a list of English words
word_list = words.words()

# Function to generate a random base64 string
def random_base64_string():
    random_bytes = random.randbytes(6)  # 6 bytes -> 8 base64 characters
    return base64.b64encode(random_bytes).decode('utf-8')

# Function to generate random data based on column type
def generate_value(col_type):
    if col_type == "boolean":
        return random.choice([True, False])
    elif col_type == "int":
        return random.randint(0, 100)
    elif col_type == "float":
        return random.uniform(0, 100)
    elif col_type == "base64":
        return random_base64_string()
    elif col_type == "string":
        return random.choice(word_list)  # Select a random word as a string

# Define column types and make sure we include base64, int, float, boolean, and string
column_types = ["boolean", "int", "float", "base64", "string"]
for _ in range(5):  # Ensure we have at least 10 columns with various types
    column_types.append(random.choice(["boolean", "int", "float", "string"]))

# Use NLTK's words to generate column names
word_list = words.words()
column_names = random.sample(word_list, len(column_types))

# Function to generate a random row with random data for the columns
def generate_row():
    row = {}
    for col_name, col_type in zip(column_names, column_types):
        if random.choice([True, False]):  # Randomly choose whether to include this column
            row[col_name] = generate_value(col_type)
    return row

# Generate and write NDJSON file with random rows
def generate_ndjson(file_name, num_rows):
    with open(file_name, 'w') as f:
        for _ in range(num_rows):
            row = generate_row()
            json.dump(row, f)
            f.write('\n')

# Example usage: Generate 100 rows of NDJSON data
generate_ndjson('mixed.ndjson', 100)