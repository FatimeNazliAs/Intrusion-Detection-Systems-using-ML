import pandas as pd
import os
import psutil
from utils import load_data_paths_from_config


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
    pd.DataFrame
        DataFrame with updated column names.
    """
    df = pd.read_csv(df_path, names=column_names)
    return df


def save_df_to_output_folder(df, df_out_path):
    """
    Save DataFrame to a specified output folder

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be saved

    df_out_path :str
        Path to save the DataFrame
    """
    df.to_csv(df_out_path, index=False)


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
    """Convert list data to csv data

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
    pandas.DataFrame
        DataFrame created from the input data.
    """
    df = pd.DataFrame(data, columns=column_names)
    df.to_csv(output_file, index=False)


def main(
    attack_free_txt_path,
    attack_free_csv_out_path,
    dos_df_in_path,
    fuzzy_df_in_path,
    dos_df_out_path,
    fuzzy_df_out_path,
    attack_free_column_names,
    dos_and_fuzzy_column_names,
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
        save_df_to_output_folder(dos_df, dos_df_out_path)
    if check_file_exists(fuzzy_df_out_path) is False:
        fuzzy_df = set_column_names(dos_and_fuzzy_column_names, fuzzy_df_in_path)
        save_df_to_output_folder(fuzzy_df, fuzzy_df_out_path)
    if check_file_exists(attack_free_csv_out_path) is False:
        attack_free_data_list = convert_attack_free_txt_to_list(attack_free_txt_path)
        convert_list_to_csv(
            attack_free_data_list, attack_free_csv_out_path, attack_free_column_names
        )


def show_memory_usage():
    process = psutil.Process()
    bytes = process.memory_info().rss
    mb = bytes / 1048576
    print("Megabyte", mb)


if __name__ == "__main__":
    dos_df_in_path, fuzzy_df_in_path, attack_free_in_path = load_data_paths_from_config
    ("in_paths")

    dos_df_out_path, fuzzy_df_out_path, attack_free_out_path = (
        load_data_paths_from_config("out_paths")
    )
    attack_free_column_names = ["timestamp", "canId", "frameType", "dlc"] + [
        f"byte{i}" for i in range(8)
    ]
    dos_and_fuzzy_column_names = (
        ["timestamp", "canId", "dlc"] + [f"byte{i}" for i in range(8)] + ["flag"]
    )


    # main(
    #     attack_free_in_path,
    #     attack_free_out_path,
    #     dos_df_in_path,
    #     fuzzy_df_in_path,
    #     dos_df_out_path,
    #     fuzzy_df_out_path,
    #     attack_free_column_names,
    #     dos_and_fuzzy_column_names,
    # )
