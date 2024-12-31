import polars as pl
import os
import pandas as pd
import time
from utils import load_data_paths


def check_file_exists(file_path):
    """
    Check if a file exists at the given path.

    Parameters
    ----------
    file_path : str
        Path to the file.

    Returns
    -------
    Boolean
        True if the file exists, False otherwise.
    """
    return os.path.isfile(file_path)


def set_column_names(column_names, df_path):
    """
    Set column names for a DataFrame read from a CSV file.

    Parameters
    ----------
    column_names list of str
        List of column names.
    df_path : str
        Path to csv file.

    Returns
    -------
    pl.DataFrame
        DataFrame with updated column names.
    """
    df = pl.read_csv(df_path)
    df.columns = column_names
    return df


def save_df_to_output_folder(df, df_out_path):
    """
    Save DataFrame to a specified output folder

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to be saved

    df_out_path :str
        Path to save the DataFrame
    """
    try:
        df.write_csv(df_out_path)
    except Exception as e:
        print(f"Failed to save DataFrame to {df_out_path}: {e}")


def convert_attack_free_txt_to_list(input_file):
    """
    Parse a text file to extract numerical data and return it as a list.

    Parameters
    ----------
    input_file : str
        Path to the text file.

    Returns
    -------
    list
        Extracted data structured for CSV conversion.
    """
    data = []
    try:
        with open(input_file, "r") as file:
            lines = file.readlines()
        for line in lines:
            if line.strip():
                if line.startswith("Timestamp:"):
                    parts = line.split()
                    if len(parts) >= 8:
                        timestamp = parts[1]
                        id_value = parts[3]
                        frame_type = parts[4]
                        dlc = parts[6]
                        bytes_data = parts[7:]
                        row = [timestamp, id_value, frame_type, dlc] + bytes_data
                        data.append(row)

    except FileNotFoundError:
        print(f"Error: File not found at {input_file}")
    except Exception as e:
        print(f"Error reading file {input_file}: {e}")
    return data


def convert_list_to_csv(data, output_file, column_names):
    """
    Convert list data to csv data

    Parameters
    ----------
    data :list
        Data was converted to list with convert_attack_free_txt_to_list().
    output_file:str
        Path to the csv file.
    column_names : list of str
        Column names to assign to the data.

    Returns
    -------
    pl.DataFrame
        DataFrame created from the input data.
    """
    df = pd.DataFrame(data, columns=column_names)
    df = pl.from_pandas(df)
    df.write_csv(output_file)
    return df


def set_new_flag_for_non_max_dlc(
    df, max_dlc_value, existing_dlc_column_name, new_flag_column_name
):
    """
    Sets new flag values for rows where `dlc` is less than the maximum value.

    Parameters
    ----------
    df : DataFrame
        The input dataframe.
    max_dlc_value : int
        The maximum value of DLC.
    existing_dlc_column_name : str
        Name of the column containing the current DLC values.
    new_flag_column_name : str
        Name of the column to store the new flag values.

    Returns
    -------
    DataFrame
        Updated dataframe with new flag values for non-maximum DLC rows.
    """
    return df.with_columns(
        pl.when(pl.col("dlc") != max_dlc_value)
        .then(
            pl.when(pl.col(existing_dlc_column_name) == 0)
            .then(pl.col("byte0"))
            .when(pl.col(existing_dlc_column_name) == 1)
            .then(pl.col("byte1"))
            .when(pl.col(existing_dlc_column_name) == 2)
            .then(pl.col("byte2"))
            .when(pl.col(existing_dlc_column_name) == 3)
            .then(pl.col("byte3"))
            .when(pl.col(existing_dlc_column_name) == 4)
            .then(pl.col("byte4"))
            .when(pl.col(existing_dlc_column_name) == 5)
            .then(pl.col("byte5"))
            .when(pl.col(existing_dlc_column_name) == 6)
            .then(pl.col("byte6"))
            .when(pl.col(existing_dlc_column_name) == 7)
            .then(pl.col("byte7"))
            .otherwise(None)
        )
        .alias(new_flag_column_name)
    )


def set_byte_to_null_if_byte_contains_flag(df, existing_dlc_column_name):
    """
    Nullifies byte columns containing misplaced flag values.

    Parameters
    ----------
    df : DataFrame
        The input dataframe.
    existing_dlc_column_name : str
        Name of the column containing the current DLC values.

    Returns
    -------
    DataFrame
        Updated dataframe with nullified byte columns containing misplaced flags.
    """

    return df.with_columns(
        [
            pl.when(pl.col(existing_dlc_column_name) == i)
            .then(None)  # Set to null if dlc matches the byte column
            .otherwise(pl.col(f"byte{i}"))  # Keep the original value otherwise
            .alias(f"byte{i}")
            for i in range(df[existing_dlc_column_name].max())  # Update the byte column
        ]
    )


def set_new_flag_for_max_dlc(
    df,
    max_dlc_value,
    existing_dlc_column_name,
    existing_flag_column_name,
    new_flag_column_name,
):
    """
    Corrects the flag column for rows where `dlc` equals the maximum value.

    Parameters
    ----------
    df : DataFrame
        The input dataframe.
    max_dlc_value : int
        The maximum value of DLC.
    existing_dlc_column_name : str
        Name of the column containing the current DLC values.
    existing_flag_column_name : str
        Name of the column containing the current flag values.
    new_flag_column_name : str
        Name of the column to store the corrected flag values.

    Returns
    -------
    DataFrame
        Updated dataframe with corrected flag values for maximum DLC rows.
    """

    return df.with_columns(
        pl.when(pl.col(existing_dlc_column_name) == max_dlc_value)
        .then(pl.col(existing_flag_column_name))
        .otherwise(pl.col(new_flag_column_name))
        .alias(new_flag_column_name)
    )


def drop_column(df, column_name):
    return df.drop(column_name)


def update_dlc_flag_association(
    df,
    existing_dlc_column_name,
    existing_flag_column_name,
    new_flag_column_name,
):
    """
    Updates flag associations by handling misplaced flags and cleaning
    byte columns, and deleting old flag columns.

    Parameters
    ----------
    df : DataFrame
        The input dataframe containing byte, flag, and DLC columns.
    max_dlc_value : int
        The maximum value of DLC.
    existing_dlc_column_name : str
        Name of the column containing the current DLC values.
    existing_flag_column_name : str
        Name of the column containing the flag values.
    new_flag_column_name : str
        Name of the column to store the updated flag values.

    Returns
    -------
    DataFrame
        Updated dataframe with corrected flag associations.
    """

    max_dlc_value = df[existing_dlc_column_name].max()
    df = set_new_flag_for_non_max_dlc(
        df, max_dlc_value, existing_dlc_column_name, new_flag_column_name
    )
    df = set_byte_to_null_if_byte_contains_flag(df, existing_dlc_column_name)
    df = set_new_flag_for_max_dlc(
        df,
        max_dlc_value,
        existing_dlc_column_name,
        existing_flag_column_name,
        new_flag_column_name,
    )
    df = drop_column(df, existing_flag_column_name)
    return df


def process_csv(
    df_name,
    df_in_path,
    column_names,
    df_out_path,
    existing_dlc_column_name,
    existing_flag_column_name,
    new_flag_column_name,
):
    """
    Processes a CSV file by applying transformations and saving the output.

    This function checks if the output file already exists. If it doesn't, it:
    1. Renames the columns of the input DataFrame.
    2. Updates the DataFrame with a new flag association based on existing columns.
    3. Saves the processed DataFrame to the specified output path.

    Parameters
    ----------
    df_name : str
        Name of the DataFrame being processed, used for logging purposes.
    df_in_path : str
        Path to the input CSV file.
    column_names : list of str
        List of new column names to assign to the DataFrame.
    df_out_path : str
        Path where the processed DataFrame will be saved.
    existing_dlc_column_name : str
        Name of the column containing DLC information.
    existing_flag_column_name : str
        Name of the column containing the existing flag information.
    new_flag_column_name : str
        Name of the new flag column to be created or updated.

    Returns
    -------
    pandas.DataFrame
        The processed DataFrame.
    """
    if check_file_exists(df_out_path) is False:
        print(f"Processing {df_name} CSV...")
        df = set_column_names(column_names, df_in_path)
        df = update_dlc_flag_association(
            df,
            existing_dlc_column_name,
            existing_flag_column_name,
            new_flag_column_name,
        )
        save_df_to_output_folder(df, df_out_path)
        print(f"{df_name} CSV is saved to output folder!")
        return df


def process_txt(df_name, df_out_path, column_names, df_in_path):
    """
    Processes a TXT file by converting it to a CSV file and saving the output.

    This function checks if the output file already exists. If it doesn't, it:
    1. Converts the content of the TXT file into a list.
    2. Converts the list into a CSV format with the specified column names.
    3. Saves the processed DataFrame to the specified output path.

    Parameters
    ----------
    df_name : str
        Name of the DataFrame being processed, used for logging purposes.
    df_out_path : str
        Path where the processed DataFrame will be saved.
    column_names : list of str
        List of column names for the resulting DataFrame.
    df_in_path : str
        Path to the input TXT file.

    Returns
    -------
    pandas.DataFrame
        The processed DataFrame.
    """
    if check_file_exists(df_out_path) is False:
        print(f"Processing {df_name} txt...")
        txt_to_list_start = time.time()
        data_list = convert_attack_free_txt_to_list(df_in_path)
        txt_to_list_end = time.time()
        # print("attack_free_txt_to_list", txt_to_list_end-txt_to_list_start)
        list_to_csv_start = time.time()
        df = convert_list_to_csv(data_list, df_out_path, column_names)
        list_to_csv_end = time.time()
        # print("attack_free_list_to_csv", list_to_csv_end-list_to_csv_start)
        print(f"{df_name} txt is saved to output folder!")
        return df


if __name__ == "__main__":
    dos_df_in_path, fuzzy_df_in_path, attack_free_in_path = load_data_paths(
        "in_paths"
    )

    dos_df_out_path, fuzzy_df_out_path, attack_free_csv_out_path = (
        load_data_paths("out_paths")
    )

    attack_free_column_names = ["timestamp", "canId", "frameType", "dlc"] + [
        f"byte{i}" for i in range(8)
    ]

    dos_and_fuzzy_column_names = (
        ["timestamp", "canId", "dlc"] + [f"byte{i}" for i in range(8)] + ["flag"]
    )
    existing_dlc_column_name = "dlc"
    existing_flag_column_name = "flag"
    new_flag_column_name = "updatedFlag"

    print("Starting DataFrame processing...")

    dos_df = process_csv(
        "DoS",
        dos_df_in_path,
        dos_and_fuzzy_column_names,
        dos_df_out_path,
        existing_dlc_column_name,
        existing_flag_column_name,
        new_flag_column_name,
    )
    fuzy_df = process_csv(
        "Fuzzy",
        fuzzy_df_in_path,
        dos_and_fuzzy_column_names,
        fuzzy_df_out_path,
        existing_dlc_column_name,
        existing_flag_column_name,
        new_flag_column_name,
    )
    attack_free_df = process_txt(
        "Attack Free",
        attack_free_csv_out_path,
        attack_free_column_names,
        attack_free_in_path,
    )
