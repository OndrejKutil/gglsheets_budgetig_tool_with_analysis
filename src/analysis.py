"""Analysis module for financial calculations and data processing.

This module provides functions for analyzing financial data including:
- Transaction summaries
- Category-based calculations
- Cashflow analysis
- Expense ratio calculations
"""

import pandas as pd
from typing import List, Tuple
import logging


# Setup logging
logger = logging.getLogger('analysis')
logger.setLevel(logging.INFO)
if not logger.handlers:
    file_handler = logging.FileHandler("app_debug.log")
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    logger.propagate = False



def get_all_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get all categories for API usage, formatted as a DataFrame with columns for each type.

    Args:
        df (DataFrame): DataFrame containing at least 'CATEGORY' and 'TYPE' columns

    Returns:
        DataFrame: DataFrame with columns 'income', 'expense', 'saving', 'investment', each containing category names
    """

    df = df[df['CATEGORY'] != 'Exclude']
    df = df[df['CATEGORY'] != 'Starting Balance']
    df = df[df['CATEGORY'] != '']
    df = df[df['CATEGORY'].notna()]
    df = df[df['CATEGORY'] != 'None']

    unique_categories = df['CATEGORY'].unique()
    logger.info(f"Unique categories found: {len(list(unique_categories))}")

    # Initialize category lists
    income_categories = []
    expense_categories = []
    saving_categories = []
    investing_categories = []

    for category in unique_categories:
        cat_type = df['TYPE'][df['CATEGORY'] == category].iloc[0]
        if cat_type == 'income':
            income_categories.append(category)
        elif cat_type == 'expense':
            expense_categories.append(category)
        elif cat_type == 'saving':
            saving_categories.append(category)
        elif cat_type == 'investment':
            investing_categories.append(category)
        else:
            logger.warning(f"Unknown category type for {category}. Please check the data.")

    logger.info(f"Retrieved categories: {len(income_categories)} income, {len(expense_categories)} expense, "
                f"{len(saving_categories)} saving, {len(investing_categories)} investment")

    # Pad lists to the same length for DataFrame construction
    max_len = max(len(income_categories), len(expense_categories), len(saving_categories), len(investing_categories))
    def pad(lst): return lst + [None] * (max_len - len(lst))

    data = {
        'income': pad(income_categories),
        'expense': pad(expense_categories),
        'saving': pad(saving_categories),
        'investment': pad(investing_categories)
    }
    return pd.DataFrame(data)


def sum_values_by_criteria(transactions: pd.DataFrame, value_column_header: str, **criteria) -> float:
    """Sum transaction values based on given criteria.
    
    Args:
        transactions: DataFrame containing financial transactions
        value_column_header: Column name containing monetary values
        **criteria: Key-value pairs for filtering (e.g., CATEGORY=['Food', 'Transport'])
                   Special key VALUE_CONDITION can be used for value comparisons (e.g., '> 0')
    
    Returns:
        float: Sum of filtered values
    """
    if transactions.empty:
        return 0.0
        
    filtered_df = transactions.copy()
    
    # Check if value_column_header exists in columns
    if value_column_header not in filtered_df.columns:
        return 0.0
    
    # First ensure we're working with strings before applying string operations
    filtered_df[value_column_header] = filtered_df[value_column_header].astype(str)
    filtered_df[value_column_header] = (filtered_df[value_column_header]
                             .str.replace('Kč', '')
                             .str.replace(',', '')
                             .str.strip()
                             .astype(float))
    
    # Special handling for VALUE_CONDITION
    if 'VALUE_CONDITION' in criteria:
        value_condition = criteria.pop('VALUE_CONDITION')
        try:
            filtered_df = filtered_df.query(f"{value_column_header} {value_condition}")
        except Exception as e:
            # If query fails, try manual filtering
            if '>' in value_condition:
                threshold = float(value_condition.replace('>', '').strip())
                filtered_df = filtered_df[filtered_df[value_column_header] > threshold]
            elif '<' in value_condition:
                threshold = float(value_condition.replace('<', '').strip())
                filtered_df = filtered_df[filtered_df[value_column_header] < threshold]
    
    # Process remaining criteria
    for key, value in criteria.items():
        if key not in filtered_df.columns:
            continue
            
        if isinstance(value, list):
            filtered_df = filtered_df[filtered_df[key].isin(value)]
        elif isinstance(value, str) and any(op in value for op in ['>', '<', '>=', '<=', '==', '!=']):
            try:
                filtered_df = filtered_df.query(f"{key} {value}")
            except Exception:
                pass
        else:
            filtered_df = filtered_df[filtered_df[key] == value]
    
    return filtered_df[value_column_header].sum()


def top_5_highest_transactions(transactions: pd.DataFrame, category: str = "", month: str = "") -> pd.DataFrame:
    """Return the top 5 highest transactions, optionally filtered by category and/or month.

    Args:
        transactions (DataFrame): DataFrame containing the transactions
        category (str, optional): Category to filter by. Defaults to "" (no filter).
        month (str, optional): Month to filter by. Defaults to "" (no filter).

    Returns:
        DataFrame: Top 5 highest transactions after filtering
    """
    filtered_df = transactions.copy()
    if category:
        filtered_df = filtered_df[filtered_df['CATEGORY'] == category]
    if month:
        filtered_df = filtered_df[filtered_df['MONTH'] == month]

    # Clean the VALUE column: remove currency symbol, spaces, and commas
    filtered_df['VALUE_NUMERIC'] = (filtered_df['VALUE']
                                    .astype(str)
                                    .str.replace('Kč', '')
                                    .str.replace(',', '')
                                    .str.strip()
                                    .astype(float))

    # Get top 5 using the numeric column
    top_5_df = filtered_df.nlargest(5, 'VALUE_NUMERIC')

    # Drop the temporary numeric column
    top_5_df = top_5_df.drop('VALUE_NUMERIC', axis=1)

    return top_5_df


def sum_expenses_by_category(df, expense_cats, month=None) -> dict:
    """Sum expenses by category, optionally filtered by month.

    Args:
        df (DataFrame): DataFrame containing the transactions
        expense_cats (list): List of expense category names
        month (str, optional): Month name to filter by. Defaults to None.

    Returns:
        dict: Dictionary with categories as keys and summed expenses as values
    """
    # First convert the values to numeric
    df = df.copy()
    # Ensure string type before string operations
    df['VALUE'] = df['VALUE'].astype(str)
    df['VALUE_NUMERIC'] = (df['VALUE']
                          .str.replace('Kč', '')
                          .str.replace(',', '')
                          .str.strip()
                          .astype(float))
    
    # Filter by month if specified
    if month:
        df = df[df['MONTH'] == month]
    
    # Only include expense categories and negative values
    df = df[
        (df['CATEGORY'].isin(expense_cats)) & 
        (df['VALUE_NUMERIC'] < 0)
    ]
    
    # Group by category and sum absolute values
    expenses = df.groupby('CATEGORY')['VALUE_NUMERIC'].sum().abs()
    
    # Convert to dictionary and filter out zero values
    expenses_dict = expenses[expenses > 0].to_dict()
    
    return expenses_dict if expenses_dict else {'No Expenses': 0}  # Return default if no expenses


def compute_cashflow(transactions: pd.DataFrame, income_categories: list, expense_categories: list, saving_categories: list, investing_categories: list, month: str = "") -> float:
    """Calculate cashflow (income - all expenses including saving and investing), optionally filtered by month.

    Args:
        transactions (DataFrame): dataframe containing the transactions
        income_categories (list): List of income category names
        expense_categories (list): List of expense category names
        saving_categories (list): List of saving category names
        investing_categories (list): List of investing category names
        month (str, optional): Month name to filter by. Defaults to None.

    Returns:
        float: Cashflow value (positive means profit, negative means loss)
    """
    if month != "":
        total_income = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=income_categories, MONTH=month)
        total_expenses = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=expense_categories + saving_categories + investing_categories, MONTH=month)
    else:
        total_income = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=income_categories)
        total_expenses = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=expense_categories + saving_categories + investing_categories)

    return total_income + total_expenses


def compute_profit(transactions: pd.DataFrame, income_categories: list, expense_categories: list, month: str = "") -> float:
    """Compute profit (income - expenses), optionally filtered by month.

    Args:
        transactions (DataFrame): DataFrame containing the transactions
        income_categories (list): List of income category names
        expense_categories (list): List of expense category names
        month (str, optional): Month name to filter by. Defaults to "" (no filter).

    Returns:
        float: Profit value (income minus expenses)
    """
    if month != "":
        total_income = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=income_categories, MONTH=month)
        total_expenses = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=expense_categories, MONTH=month)
    else:
        total_income = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=income_categories)
        total_expenses = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=expense_categories)

    return total_income + total_expenses


def calculate_expense_ratio(transactions: pd.DataFrame, income_categories: list, expense_categories: list, month: str = "") -> float:
    """Calculate the ratio of expenses to income (expenses/income).

    Args:
        transactions (DataFrame): dataframe containing the transactions
        income_categories (list): List of income category names
        expense_categories (list): List of expense category names
        month (str, optional): Month name to filter by. Defaults to None.

    Returns:
        float: Expense ratio (0.7 means spending 70% of income)
    """
    if month != "":
        total_income = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=income_categories, MONTH=month)
        total_expenses = abs(sum_values_by_criteria(transactions, 'VALUE', CATEGORY=expense_categories, MONTH=month))
    else:
        total_income = sum_values_by_criteria(transactions, 'VALUE', CATEGORY=income_categories)
        total_expenses = abs(sum_values_by_criteria(transactions, 'VALUE', CATEGORY=expense_categories))
    
    if total_income == 0:
        return float('inf')
    
    return total_expenses / total_income


def sum_amount_in_each_account(transactions: pd.DataFrame) -> pd.DataFrame:
    """Sum the amount in each account.

    Args:
        transactions (DataFrame): dataframe containing the transactions

    Returns:
        DataFrame: DataFrame with account names and their corresponding total amounts
    """
    # Ensure string type before string operations
    transactions['VALUE'] = transactions['VALUE'].astype(str)
    
    # Clean the VALUE column: remove currency symbol, spaces, and commas
    transactions['VALUE_NUMERIC'] = (transactions['VALUE']
                                  .str.replace('Kč', '')
                                  .str.replace(',', '')
                                  .str.strip()
                                  .astype(float))
    
    # Group by account and sum the values
    account_sums = transactions.groupby('ACCOUNT')['VALUE_NUMERIC'].sum().reset_index()
    
    return account_sums

