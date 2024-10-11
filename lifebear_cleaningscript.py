import pandas as pd

# Define the number of chunks (4 equal parts)
num_chunks = 4

# Load the dataset
df = pd.read_csv("/content/lifebear.csv.csv", sep=';', low_memory=True)

# Determine the chunk size based on the length of the dataset
chunk_size = len(df) // num_chunks

# Split the dataset into 4 chunks and save each chunk as a separate CSV file
for i in range(num_chunks):
    start_index = i * chunk_size
    end_index = (i + 1) * chunk_size if i < num_chunks - 1 else len(df)
    chunk = df.iloc[start_index:end_index]
    chunk.to_csv(f'/content/lifebear_dataset_chunk_{i+1}.csv', index=False)

print(f"The CSV file has been divided into {num_chunks} smaller portions.")
import pandas as pd
import re

# Function to check for invalid data (null or empty)
def is_invalid(value):
    return pd.isnull(value) or str(value).strip() == ''

# Function to validate if 'created_at' follows the correct format
def is_valid_created_at(value):
    pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'  # Format: year-mm-dd hh:mm:ss
    return bool(re.match(pattern, str(value)))

# Function to validate if 'birthday_on' follows the correct format
def is_valid_birthday_on(value):
    pattern = r'^\d{4}-\d{2}-\d{2}$'  # Format: year-mm-dd
    return bool(re.match(pattern, str(value)))

# Function to validate if 'gender' is either 1 or 0
def is_valid_gender(value):
    return value in [0, 1]

# Load the dataset
df = pd.read_csv("/content/lifebear.csv.csv", sep=';', low_memory=True)

# Define the number of chunks (4 equal parts)
num_chunks = 4
chunk_size = len(df) // num_chunks

# List of columns to check for null/empty values
columns_to_check = ['id', 'login_id', 'mail_address', 'password', 'created_at', 'salt', 'birthday_on', 'gender']

# Process each chunk
for i in range(num_chunks):
    start_index = i * chunk_size
    end_index = (i + 1) * chunk_size if i < num_chunks - 1 else len(df)

    # Extract chunk
    chunk = df.iloc[start_index:end_index]

    # Create a mask to find invalid data
    invalid_data_mask = chunk[columns_to_check].applymap(is_invalid).any(axis=1)

    # Check for invalid 'created_at', 'birthday_on', and 'gender'
    invalid_created_at_mask = ~chunk['created_at'].apply(is_valid_created_at)
    invalid_birthday_on_mask = ~chunk['birthday_on'].apply(is_valid_birthday_on)
    invalid_gender_mask = ~chunk['gender'].apply(is_valid_gender)

    # Combine all invalid masks
    total_invalid_mask = invalid_data_mask | invalid_created_at_mask | invalid_birthday_on_mask | invalid_gender_mask

    # Separate corrupt data
    corrupt_data = chunk[total_invalid_mask]

    # Separate clean data
    clean_data = chunk[~total_invalid_mask]

    # Save corrupt data if any
    if not corrupt_data.empty:
        corrupt_data.to_csv(f'/content/corrupt_data_chunk_{i+1}.csv', index=False)
        print(f"Corrupt data found and saved to 'corrupt_data_chunk_{i+1}.csv'. Total rows: {len(corrupt_data)}")

    # Save the clean chunk data
    clean_data.to_csv(f'/content/lifebear_dataset_chunk_{i+1}.csv', index=False)

print(f"The CSV file has been divided into {num_chunks} smaller portions and corrupt data has been separated.")
import pandas as pd
import re

try:
    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv('/content/lifebear_dataset_chunk_1.csv')

    # Remove duplicate email addresses based on 'mail_address' column
    df_no_duplicates = df.drop_duplicates(subset=['mail_address'])

    # Optionally, save the removed duplicate rows for reference
    duplicate_emails = df[df.duplicated(subset=['mail_address'])]
    duplicate_emails.to_csv('duplicate_emails.csv', index=False)
    print(f"Removed {len(duplicate_emails)} duplicate email addresses and saved them to 'duplicate_emails.csv'.")

    # Now working with the DataFrame without duplicates
    df = df_no_duplicates

    # List of columns to check for null/empty values
    columns_to_check = ['id', 'login_id', 'mail_address', 'password', 'created_at', 'salt', 'birthday_on', 'gender']

    # Create a mask that checks for any null or empty values in the specified columns
    invalid_data_mask = df[columns_to_check].applymap(is_invalid).any(axis=1)

    # Extract rows where any of the specified columns contain null/empty values
    corrupt_data = df[invalid_data_mask]

    # Save the corrupt data to a new CSV file for reference
    corrupt_data.to_csv('corrupt_data.csv', index=False)
    print("Corrupt data separated and saved to 'corrupt_data.csv'")

    # Remove the corrupt rows from the original DataFrame
    clean_data = df[~invalid_data_mask]

    # Overwrite the original CSV file with the cleaned data
    clean_data.to_csv('/content/lifebear_dataset_chunk_1.csv', index=False)
    print("The original file has been updated without the corrupt rows.")

except FileNotFoundError:
    print("File not found at the specified path.")
except Exception as e:
    print(f"An error occurred: {e}")
import pandas as pd
import re

try:
    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv('/content/lifebear_dataset_chunk_4.csv')

    # Remove duplicate email addresses based on 'mail_address' column
    df_no_duplicates = df.drop_duplicates(subset=['mail_address'])

    # Optionally, save the removed duplicate rows for reference
    duplicate_emails = df[df.duplicated(subset=['mail_address'])]
    duplicate_emails.to_csv('duplicate_emails.csv', index=False)
    print(f"Removed {len(duplicate_emails)} duplicate email addresses and saved them to 'duplicate_emails.csv'.")

    # Now working with the DataFrame without duplicates
    df = df_no_duplicates

    # Proceed with the rest of your invalid/corrupt data checks

    # List of columns to check for null/empty values
    columns_to_check = ['id', 'login_id', 'mail_address', 'password', 'created_at', 'salt', 'birthday_on', 'gender']

    # Create a mask that checks for any null or empty values in the specified columns
    invalid_data_mask = df[columns_to_check].applymap(is_invalid).any(axis=1)

    # Check for valid 'created_at' format
    invalid_created_at_mask = ~df['created_at'].apply(is_valid_created_at)

    # Check for valid 'birthday_on' format
    invalid_birthday_on_mask = ~df['birthday_on'].apply(is_valid_birthday_on)

    # Check for valid 'gender' values
    invalid_gender_mask = ~df['gender'].apply(is_valid_gender)

    # Combine all invalid masks
    total_invalid_mask = invalid_data_mask | invalid_created_at_mask | invalid_birthday_on_mask | invalid_gender_mask

    # Extract rows that contain invalid data
    corrupt_data = df[total_invalid_mask]

    # Save the corrupt data to a new CSV file
    corrupt_data.to_csv('corrupt_data.csv', index=False)
    print("Corrupt data separated and saved to 'corrupt_data.csv'")

    # Remove the corrupt rows from the original DataFrame
    clean_data = df[~total_invalid_mask]

    # Overwrite the original CSV file with the cleaned data
    clean_data.to_csv('/content/lifebear_dataset_chunk_5.csv', index=False)
    print("The original file has been updated without corrupt or invalid rows.")

except FileNotFoundError:
    print("File not found at the specified path.")
except Exception as e:
    print(f"An error occurred: {e}")
import pandas as pd

# List of chunk file paths
chunk_files = [
    '/content/lifebear_dataset_chunk_1.csv',
    '/content/lifebear_dataset_chunk_2.csv',
    '/content/lifebear_dataset_chunk_3.csv',
    '/content/lifebear_dataset_chunk_4.csv',
    '/content/lifebear_dataset_chunk_5.csv'
]

# Function to replace 0.0 and 1.0 with 'male' and 'female' in the gender column
def replace_gender(chunk):
    chunk['gender'] = chunk['gender'].replace({0.0: 'male', 1.0: 'female'})
    return chunk

# Loop through each chunk, modify it, and save it back
for file_path in chunk_files:
    # Load the chunk
    df = pd.read_csv(file_path)

    # Replace gender values
    df = replace_gender(df)

    # Save the modified chunk back to CSV
    df.to_csv(file_path, index=False)
    print(f"Updated {file_path} and saved.")
# List to hold all the modified chunks
chunks = []

# Load each modified chunk and append to the list
for file_path in chunk_files:
    df = pd.read_csv(file_path)
    chunks.append(df)

# Concatenate all the chunks into one DataFrame
combined_data = pd.concat(chunks)

# Save the combined DataFrame into a new CSV file
combined_data.to_csv('/content/lifebear_dataset_combined.csv', index=False)

print("All chunks have been combined and saved as 'lifebear_dataset_combined.csv'")
