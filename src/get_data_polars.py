import polars as pl
import os
import pandas as pd
import psutil


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
    if os.path.isfile(file_path):
        return True
    return False


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
    df.write_csv(df_out_path)


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
    with open(input_file, "r") as file:
        lines = file.readlines()
    data = []
    for line in lines:
        if line.strip():
            if line.startswith("Timestamp:"):
                # Parse lines starting with "Timestamp"
                parts = line.split()
                if len(parts) >= 8:
                    # The check ensures the line contains all these expected
                    # parts, preventing errors when trying to access specific
                    # elements in the list.
                    timestamp = parts[1]
                    id_value = parts[3]
                    frame_type = parts[4]
                    dlc = parts[6]
                    bytes_data = parts[7:]
                    row = [timestamp, id_value, frame_type, dlc] + bytes_data
                    data.append(row)
                else:
                    continue
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

    for i in range(8):
        df = df.with_columns(
            pl.when(pl.col(existing_dlc_column_name) == i)
            .then(None)  # Set to null if dlc matches the byte column
            .otherwise(pl.col(f"byte{i}"))  # Keep the original value otherwise
            .alias(f"byte{i}")  # Update the byte column
        )
    return df


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


def main(
    attack_free_txt_path,
    attack_free_csv_out_path,
    dos_df_in_path,
    fuzzy_df_in_path,
    dos_df_out_path,
    fuzzy_df_out_path,
    attack_free_column_names,
    dos_and_fuzzy_column_names,
    existing_dlc_column_name,
    existing_flag_column_name,
    new_flag_column_name,
):
    """
    Process input data and save to output paths if they don't exist.

    Parameters
    ----------
    attack_free_txt_path : str
        Path to the attack-free text file.
    attack_free_csv_out_path : str
        Output path for the converted attack-free CSV file.
    dos_df_in_path : str
        Input path for the DOS data.
    fuzzy_df_in_path : str
        Input path for the fuzzy data.
    dos_df_out_path : str
        Output path for the dos DataFrame.
    fuzzy_df_out_path : str
        Output path for the fuzzy DataFrame.
    attack_free_column_names : list of str
        Column names for the attack-free data.
    dos_and_fuzzy_column_names : list of str
        Column names for the DOS and fuzzy data.
    """

    if check_file_exists(dos_df_out_path) is False:
        dos_df = set_column_names(dos_and_fuzzy_column_names, dos_df_in_path)
        dos_df = update_dlc_flag_association(
            dos_df,
            existing_dlc_column_name,
            existing_flag_column_name,
            new_flag_column_name,
        )
        save_df_to_output_folder(dos_df, dos_df_out_path)
    if check_file_exists(fuzzy_df_out_path) is False:
        fuzzy_df = set_column_names(dos_and_fuzzy_column_names, fuzzy_df_in_path)
        fuzzy_df = update_dlc_flag_association(
            fuzzy_df,
            existing_dlc_column_name,
            existing_flag_column_name,
            new_flag_column_name,
        )
        save_df_to_output_folder(fuzzy_df, fuzzy_df_out_path)
    if check_file_exists(attack_free_csv_out_path) is False:
        attack_free_data_list = convert_attack_free_txt_to_list(attack_free_txt_path)
        convert_list_to_csv(
            attack_free_data_list, attack_free_csv_out_path, attack_free_column_names
        )
    print("Completed Main...")


def show_memory_usage():
    process = psutil.Process()
    bytes = process.memory_info().rss
    mb = bytes / 1048576
    print("Megabyte", mb)


if __name__ == "__main__":
    attack_free_txt_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/input/attack_free.txt"
    )
    attack_free_csv_out_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/output/attack_free_df.csv"
    )

    dos_df_in_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/input/dos_dataset.csv"
    )
    fuzzy_df_in_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/input/fuzzy_dataset.csv"
    )
    dos_df_out_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/output/dos_df.csv"
    )
    fuzzy_df_out_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/output/fuzzy_df.csv"
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

    main(
        attack_free_txt_path,
        attack_free_csv_out_path,
        dos_df_in_path,
        fuzzy_df_in_path,
        dos_df_out_path,
        fuzzy_df_out_path,
        attack_free_column_names,
        dos_and_fuzzy_column_names,
        existing_dlc_column_name,
        existing_flag_column_name,
        new_flag_column_name,
    )
