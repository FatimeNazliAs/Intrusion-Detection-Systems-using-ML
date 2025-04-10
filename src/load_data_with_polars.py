"""
Workflow of load data
1. datasets in input folder are uploaded
2. dos and fuzzy attacks are in csv format
3. attack free is in txt format
4. process_csv() method is used for dos and fuzzy.
    1. it checks whether csv file exists in output folder
        1. if not
            1. set column names
            2. fix dlc- flag issue (update_dlc_flag_association())
            3. save updated pl df into output folder
5. process_txt method is used for attack free df.
    1. it checks whether csv file exists in output folder
        1. if not
            1. convert txt file to list format by deleting column names from each line in txt
            2. convert list to csv
            3. save updated pl df into output folder
"""

import polars as pl
import pandas as pd
from utils import (
    load_data_paths,
    check_file_exists,
    set_column_names,
    save_df_to_csv,
)


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
            .then(pl.col("byte_0"))
            .when(pl.col(existing_dlc_column_name) == 1)
            .then(pl.col("byte_1"))
            .when(pl.col(existing_dlc_column_name) == 2)
            .then(pl.col("byte_2"))
            .when(pl.col(existing_dlc_column_name) == 3)
            .then(pl.col("byte_3"))
            .when(pl.col(existing_dlc_column_name) == 4)
            .then(pl.col("byte_4"))
            .when(pl.col(existing_dlc_column_name) == 5)
            .then(pl.col("byte_5"))
            .when(pl.col(existing_dlc_column_name) == 6)
            .then(pl.col("byte_6"))
            .when(pl.col(existing_dlc_column_name) == 7)
            .then(pl.col("byte_7"))
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
            .otherwise(pl.col(f"byte_{i}"))  # Keep the original value otherwise
            .alias(f"byte_{i}")
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


def update_dlc_flag_association(
    df,
    existing_dlc_column_name,
    existing_flag_column_name,
    new_flag_column_name,
):
    """
    Updates flag associations by handling misplaced flags and cleaning byte columns, and deleting old flag columns.

    dlc-flag issue:
        - if dlc is 8 (at most), flag column value will be in flag column.
        - if dlc is less than 8 for example 2, flag column value will be in byte_2 column. Because first two byte
            columns (byte_0 and byte_1 will be full with byte values and rest byte columns will be empty.). That's
            why we need to check whether flag value is in right column or not!

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
    df = df.drop(existing_flag_column_name)
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
    Processes a CSV file by transforming and saving it to a specified output path.

    This function checks if the output file already exists:
    - If the file exists, it returns the DataFrame from the existing CSV.
    - If the file does not exist, it performs the following steps:
        1. Renames the columns of the input DataFrame based on the provided `column_names`.
        2. Updates the DataFrame by associating the new flag column with the values from the existing columns (`existing_dlc_column_name` and `existing_flag_column_name`).
        3. Saves the processed DataFrame to the specified output file path.


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

    if check_file_exists(df_out_path):
        return pl.read_csv(df_out_path)

    else:
        print(f"Processing {df_name} CSV...")
        df = set_column_names(column_names, df_in_path, backend="polars")
        df = update_dlc_flag_association(
            df,
            existing_dlc_column_name,
            existing_flag_column_name,
            new_flag_column_name,
        )
        save_df_to_csv(df, df_out_path, backend="polars")
        print(f"{df_name} CSV is saved to output folder!")
        return df


def process_txt(df_name, df_out_path, column_names, df_in_path):
    """
    Processes a TXT file by converting it to a CSV file and saving the output.

    This function checks if the output file already exists:
    - If the output file exists, it returns the DataFrame from the existing CSV.
    - If the file doesn't exist, it performs the following steps:
        1. Converts the content of the input TXT file into a list.
        2. Converts the list into a DataFrame with the specified column names.
        3. Saves the processed DataFrame as a CSV file to the specified output path.


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
    if check_file_exists(df_out_path):
        return pl.read_csv(df_out_path)
    else:
        print(f"Processing {df_name} txt...")
        data_list = convert_attack_free_txt_to_list(df_in_path)
        df = convert_list_to_csv(data_list, df_out_path, column_names)
        print(f"{df_name} txt is saved to output folder!")
        return df


if __name__ == "__main__":

    input_data_paths = load_data_paths("in_paths")
    dos_df_in_path = input_data_paths["dos_df"]
    fuzzy_df_in_path = input_data_paths["fuzzy_df"]
    attack_free_in_path = input_data_paths["attack_free_df"]

    output_data_paths = load_data_paths("out_paths")

    dos_df_out_path = output_data_paths["dos_df"]
    fuzzy_df_out_path = output_data_paths["fuzzy_df"]
    attack_free_df_out_path = output_data_paths["attack_free_df"]

    dos_and_fuzzy_column_names = (
        ["timestamp", "can_id", "dlc"] + [f"byte_{i}" for i in range(8)] + ["flag"]
    )

    attack_free_column_names = ["timestamp", "can_id", "frame_type", "dlc"] + [
        f"byte_{i}" for i in range(8)
    ]

    existing_dlc_column_name = "dlc"
    existing_flag_column_name = "flag"
    new_flag_column_name = "updated_flag"

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
        attack_free_df_out_path,
        attack_free_column_names,
        attack_free_in_path,
    )
    stratified_sample_size = 20000
    random_sample_size = 20000

    print("dos_df", dos_df.shape)
    print("fuzy_df", fuzy_df.shape)
    print("attack_free_df", attack_free_df.shape)
