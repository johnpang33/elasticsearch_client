import os
import csv
import json
import pandas as pd
import numpy as np

def parse_es_response(response):
    """
    Parse an Elasticsearch response to extract total hits and all `_source` fields.
    
    Args:
        response (dict): The Elasticsearch response object.
        
    Returns:
        dict: A dictionary with `total_hits` and `hits_sources` keys.
              `total_hits` is the total number of hits.
              `hits_sources` is a list of `_source` fields for each hit.
    """
    try:
        # Extract total hits
        total_hits = response.get("hits", {}).get("total", {}).get("value", 0)
        # Extract all `_source` fields from hits
        hits_sources = [hit.get("_source", {}) for hit in response.get("hits", {}).get("hits", [])]
        
        return {
            "total_hits": total_hits,
            "hits_sources": hits_sources,
        }
    except Exception as e:
        print(f"Error parsing Elasticsearch response: {e}")
        return {
            "total_hits": 0,
            "hits_sources": [],
        }

def rename_columns_in_dataframe(df, rename_file):
    """
    Rename columns in the dataframe based on a source of truth (JSON file).

    :param df: DataFrame to be processed
    :param rename_file: JSON file that maps old column names to new names (default: "rename_columns.json")
    :return: DataFrame with renamed columns
    """

    # Step 1: Load the column rename mapping from the JSON file
    try:
        with open(rename_file, "r") as fr:
            cont = fr.read()
            rename_mapping = json.loads(cont)
    except FileNotFoundError:
        print(f"Error: The file {rename_file} was not found.")
        return df
    
    # Step 2: Rename the columns in the dataframe
    df.rename(columns=rename_mapping, inplace=True)
    print(f'Renamed all columns, {len(df)} records.')

    # Return the modified dataframe
    return df

def load_mapping_from_json(file_path):
    '''To load the es mappings from JSON file'''
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The mapping file was not found: {file_path}")
    with open(file_path, 'r', encoding="utf-8") as f:
        return json.load(f)

def convert_integer_dataframe(df, integer_columns):
    """
    This function cleans the dataframe by filling missing values, replacing specified values, 
    and converting columns to integers.

    :param df: DataFrame to be cleaned
    :param columns_to_replace: List of column names to replace NaN with 'None'
    :return: Cleaned DataFrame
    """
    try:
        if integer_columns is not None:
            for col in integer_columns:
                if col in df.columns:
                    # Replace NaN with None (or a specific integer value if preferred)
                    df[col] = df[col].replace(np.nan, None)
                    df[col] = df[col].replace('NA', None)
                    df[col] = df[col].replace('N', None)
                    df[col] = df[col].replace('', None)
            print(f'Cleaned all INTEGER columns, (rows, columns): {df.shape}.')
        else:
            print(f'No INTEGER columns, (rows, columns): {df.shape}.')
                
    except Exception as err:
        print(f"Error occurred: {err}")
    
    return df

def fillna_df(df, integer_columns):
    '''Fill NA with -'''
    if not integer_columns:
        non_int_columns = df.columns  # All columns are non-integer
    else:
        non_int_columns = [col for col in df.columns if col not in integer_columns]
        
    for col in non_int_columns:
        df[col] = df[col].replace(['', pd.NA, float('nan'), 'nan'], None)
    print(f'Filled all columns with NA to " ", (rows, columns): {df.shape}.')
    return df

def format_date_columns(df, date_columns):
    """
    This function applies a text cleaning model to specified columns and converts date columns to 
    a standardized datetime format in the dataframe.

    :param df: DataFrame to be processed
    :param text_columns: List of column names that should be cleaned with `clean_text_model`
    :param date_columns: List of column names that should be converted to datetime and formatted
    :return: DataFrame with cleaned text and formatted dates
    """

    # Apply text cleaning model to the specified columns
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].replace('-', None)

    # Return the modified DataFrame
    print(f'Formatted all DATE columns, (rows, columns): {df.shape}.')

    return df

def documents_to_csv(documents, file_path):
    """
    Save a list of documents to a CSV file.

    Args:
        documents (list[dict]): A list of dictionaries representing the documents.
        file_path (str): The path to the CSV file to save the data.

    Returns:
        bool: True if the operation is successful, False otherwise.
    """
    try:
        if not documents:
            print("No documents to save.")
            return False

        # Get all unique keys from the documents for CSV header
        header = set()
        for doc in documents:
            header.update(doc.keys())
        header = list(header)

        # Write documents to CSV
        with open(file_path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()
            writer.writerows(documents)

        print(f"Documents successfully saved to {file_path}.")
        return True
    except Exception as e:
        print(f"Error saving documents to CSV: {e}")
        return False





