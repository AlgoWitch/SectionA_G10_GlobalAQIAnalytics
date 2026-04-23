import pandas as pd
import numpy as np
import os

# --- Configuration ---
# Where the messy data lives and where we want the clean stuff to go
RAW_DATA = "data/raw/global_air_quality_datacleaning.csv"
CLEAN_DATA = "data/processed/clean_air_data.csv"

def run_etl():
    """
    Main pipeline function that cleans up the global air quality dataset.
    It handles typos, missing values, outliers, and formatting.
    """
    # Safety first: make sure we actually have a file to work with
    if not os.path.exists(RAW_DATA):
        print(f"Error: {RAW_DATA} not found.")
        return

    # Load the dataset
    df = pd.read_csv(RAW_DATA)
    print(f"Starting ETL. Row count: {len(df)}")

    # Standardize text: remove extra spaces and make everything lowercase
    # This prevents 'India' and 'india ' from being treated as different places
    df['Country'] = df['Country'].str.strip().str.lower()
    df['City'] = df['City'].str.strip().str.lower()
    
    # Catch common placeholders for missing data and turn them into proper NaNs (Not a Number)
    df.replace(['-', '?', 'missing', 'unknown', '#ref!'], np.nan, inplace=True)

    # Manual cleanup for common country typos we found in the data
    corrections = {
        'inda': 'india', 'chnia': 'china', 'inida': 'india', 'cihna': 'china', 
        'nigeira': 'nigeria', 'sout hafrica': 'south africa', 'iarn': 'iran', 
        'sapin': 'spain', 'colmobia': 'colombia', 'vietanm': 'vietnam',
        'tahiland': 'thailand', 'agrentina': 'argentina', 'argentnia': 'argentina',
        'candaa': 'canada', 'ukx': 'uk', 'norawy': 'norway', 'mongloia': 'mongolia',
        'mxeico': 'mexico', 'fracne': 'france', 'tanzaina': 'tanzania',
        'bnagladesh': 'bangladesh', 'sauid arabia': 'saudi arabia',
        'gremany': 'germany', 'uaex': 'uae', 'braizl': 'brazil', 'usax': 'usa',
        'eygpt': 'egypt', 'ngieria': 'nigeria'
    }
    df['Country'].replace(corrections, inplace=True)

    # Manual cleanup for city typos as well
    city_corrections = {
        'kolktaa': 'kolkata', 'vanocuver': 'vancouver', 'mainla': 'manila',
        'madird': 'madrid', 'marsielle': 'marseille', 'yokhoama': 'yokohama',
        'sao pualo': 'sao paulo', 'tamlae': 'tamale', 'barsa': 'barcelona',
        'ngaoya': 'nagoya', 'vatseras': 'vasteras', 'al ian': 'al ain',
        'pzonan': 'poznan', 'turjillo': 'trujillo', 'antofagsata': 'antofagasta',
        'oaska': 'osaka', 'niarobi': 'nairobi', 'rwaalpindi': 'rawalpindi',
        'kmuasi': 'kumasi', 'shnaghai': 'shanghai', 'joahnnesburg': 'johannesburg',
        'brsailia': 'brasilia', 'amstredam': 'amsterdam', 
        'saint peterbsurg': 'saint petersburg', 'buchraest': 'bucharest',
        'adids ababa': 'addis ababa'
    }
    df['City'].replace(city_corrections, inplace=True)

    # Define which columns should be numbers
    numeric_cols = [
        'AQI', 'PM2.5', 'PM10', 'Deforestation_Rate_%', 'Afforestation_Rate_%',
        'Vehicles_Increase_%', 'Industries_Increase_%', 'Env_Budget_Million_USD',
        'Population_Density_Per_SqKm', 'CO2_Emissions_MT',
        'Green_Space_Ratio_%', 'Avg_Life_Expectancy_Index'
    ]

    # Convert columns to numeric, and if it's not a number (like text in a number col), make it NaN
    # Also, we know air quality and budget can't be negative, so we treat <= 0 as invalid
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df.loc[df[col] <= 0, col] = np.nan
    
    # Year validation: keep it within a reasonable range (2000-2025)
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    df.loc[(df['Year'] < 2000) | (df['Year'] > 2025), 'Year'] = np.nan

    # Fix a specific issue: Env Budget sometimes has values that are 100x too large
    if 'Env_Budget_Million_USD' in df.columns:
        df.loc[df['Env_Budget_Million_USD'] > 10000, 'Env_Budget_Million_USD'] /= 100

    # Outlier Removal: Using the Interquartile Range (IQR) method
    # This filters out values that are extremely far from the average
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        df.loc[(df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR)), col] = np.nan

    # Drop rows where we don't even know the Country or City - they aren't useful for this analysis
    df = df.dropna(subset=['Country', 'City'])

    # Fill in missing values using the median for that specific country
    # This is more accurate than just filling with a global average
    country_cols = ['AQI', 'PM2.5', 'PM10', 'CO2_Emissions_MT', 'Green_Space_Ratio_%']
    for col in country_cols:
        df[col] = df.groupby('Country')[col].transform(lambda x: x.fillna(x.median()))

    # For Year, just use the global median if it's missing
    df['Year'] = df['Year'].fillna(df['Year'].median())
    
    # For these, fill missing values based on the median for that specific year
    year_cols = ['Vehicles_Increase_%', 'Industries_Increase_%', 'Env_Budget_Million_USD']
    for col in year_cols:
        df[col] = df.groupby('Year')[col].transform(lambda x: x.fillna(x.median()))

    # Last resort: fill any remaining NaNs with the overall median
    remaining = ['Deforestation_Rate_%', 'Afforestation_Rate_%', 'Population_Density_Per_SqKm', 'Avg_Life_Expectancy_Index']
    for col in remaining:
        df[col] = df[col].fillna(df[col].median())

    # Formatting for the final CSV: snake_case column names and integer years
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df['year'] = df['year'].astype(int)
    
    # Remove any identical rows that might have snuck in
    df = df.drop_duplicates()

    # Save the cleaned data (creating the directory if it doesn't exist)
    os.makedirs(os.path.dirname(CLEAN_DATA), exist_ok=True)
    df.to_csv(CLEAN_DATA, index=False)
    
    print(f"ETL Successful. Processed data saved with {len(df)} rows.")

if __name__ == "__main__":
    run_etl()