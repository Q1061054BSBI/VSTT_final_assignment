# Data Cleaning and Preparation Script for Olist E-commerce Dataset

This script is designed for data preprocessing and cleaning of the Olist Brazilian E-commerce dataset, consolidating data across multiple tables and preparing it for analysis.

## Features

1. **Data Import and Merging**:
   - Loads multiple CSV files, including customer, order, product, and review data from the [Olist dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
   - Merges data on key fields to create a unified dataset for analysis.

2. **Data Cleaning**:
   - **Numerical Cleaning**: Filters out outliers and negative values in numerical columns such as price and freight cost.
   - **Date Validation**: Converts date columns, removes invalid and future dates, and ensures date order correctness.
   - **String Standardization**: Capitalizes text in certain fields and converts state abbreviations to full names.
   - **Redundancy Removal**: Eliminates duplicate entries and consolidates order quantities for repeated items.

3. **ID Replacement**:
   - Converts unique IDs to numerical sequences for optimized data processing.

## Usage

1. **Prerequisites**: Ensure `pandas` is installed.
2. **Execution**: Run the script using the command:
   ```bash
   python3 main.py

**Python Version**: The code has been tested with Python version 3.12.0.

**Notes**: This code was developed for educational purposes, with all modifications tailored to the goals of this learning project. The dataset required for this script can be found [here on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
