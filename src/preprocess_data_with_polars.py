import polars as pl
from utils import load_data_paths, drop_columns, read_datasets, save_df_to_csv


def validate_column_in_dataframe(df, column_name):
    """
    Checks column exist or not in given df.

    Parameters
    ----------
    df :pl.DataFrame
        Input DataFrame.
    column_name : str
        Column name that will be checked.

    Raises
    ------
    ValueError
       If the specified column does not exist in the DataFrame.
    """

    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame.")


def convert_timestamp_to_datetime(df, new_column_name, existing_column_name):
    """
    Convert float timestamp column into datetime and add as new column.

    Parameters
    ----------
    data : pl.DataFrame
        Input DataFrame containing the timestamp column.
    new_column_name : str
        Name of new column to be added.
    existing_column_name : str
        Name of the existing column containing timestamps.

    Returns
    -------
    pl.DataFrame
        DataFrame with newly added datetime column.

    Raises
    ------
    ValueError
        If the existing column is not found in the DataFrame.
    """

    validate_column_in_dataframe(df, existing_column_name)
    df = df.with_columns(
        pl.from_epoch(pl.col(existing_column_name), time_unit="s").alias(
            new_column_name
        )
    )
    return df


def convert_multiple_dfs_timestamp_to_datetime(
    dfs, new_column_name, existing_column_name
):
    """
    Convert multiple dfs' timestamp column into datetime column.

    Parameters
    ----------
    dfs : list
        List of DataFrame.
    new_column_name : str
        Name of new column to be added
    existing_column_name : str
        Name of the existing column containing timestamps.

    Returns
    -------
    list
        List of updated DataFrame.
    """
    return [
        convert_timestamp_to_datetime(df, new_column_name, existing_column_name)
        for df in dfs
    ]


def convert_hex_column_to_int(df, new_column_name, existing_column_name):
    """
    Convert hex that it's dtype is str into int column.

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame containing the hex column.
    new_column_name : str
        Name of new column to be added.
    existing_column_name : str
        Name of the existing column containing hex.

    Returns
    -------
    pl.DataFrame
        DataFrame with newly added hex int column.

    Raises
    ------
    ValueError
        If the existing column is not found in the DataFrame.
    """

    validate_column_in_dataframe(df, existing_column_name)
    return df.with_columns(
        pl.col(existing_column_name)
        .str.to_integer(base=16, strict=True)
        .alias(new_column_name)
    )


def convert_multiple_dfs_str_hex_can_id_to_int(
    dfs, new_column_name, existing_column_name
):
    """
    Convert multiple dfs' str hex column into int hex column.

    Parameters
    ----------
    dfs : list
        List of DataFrame.
    new_column_name : str
        Name of new column to be added
    existing_column_name : str
        Name of the existing column containing hex.

    Returns
    -------
    list
        List of updated DataFrame.
    """
    return [
        convert_hex_column_to_int(df, new_column_name, existing_column_name)
        for df in dfs
    ]


def convert_bytes_to_int(df, existing_byte_column_names, new_byte_column_names):
    """
    Convert byte columns in hexadecimal format to integer columns.

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame containing the byte columns.
    existing_byte_column_names : list of str
        List of names of the existing byte columns in hexadecimal format.
    new_byte_column_names : list of str
        List of names for the new columns to store the converted integer values.

    Returns
    -------
    pl.DataFrame
        A DataFrame with the newly added integer columns corresponding to the byte columns.

    Raises
    ------
    ValueError
        If the lengths of `existing_byte_column_names` and `new_byte_column_names` do not match.
    """

    if len(existing_byte_column_names) != len(new_byte_column_names):
        raise ValueError(
            "The lengths of existing_column_names and new_column_names must match."
        )

    column_names_dict = dict(zip(existing_byte_column_names, new_byte_column_names))
    for existing_byte_column_name, new_byte_column_name in column_names_dict.items():
        df = df.with_columns(
            pl.col(existing_byte_column_name)
            .str.to_integer(base=16, strict=True)
            .alias(new_byte_column_name)
        )
    return df


def convert_multiple_dfs_bytes_to_int(dfs, existing_column_names, new_column_names):
    """
    Convert byte columns in hexadecimal format to integer columns for multiple DataFrames.

    This function applies the `convert_bytes_to_int` function to a list of DataFrames,
    transforming specified byte columns in hexadecimal format into integer columns.

    Parameters
    ----------
    dfs : list of pl.DataFrame
        A list of DataFrames, each containing the byte columns to be converted.
    existing_column_names : list of str
        List of names of the existing byte columns in each DataFrame.
    new_column_names : list of str
        List of names for the new columns to store the converted integer values in each DataFrame.

    Returns
    -------
    list of pl.DataFrame
        A list of DataFrames with the newly added integer columns corresponding to the byte columns.

    Raises
    ------
    ValueError
        If the lengths of `existing_column_names` and `new_column_names` do not match.
    """
    return [
        convert_bytes_to_int(df, existing_column_names, new_column_names) for df in dfs
    ]


def merge_byte_columns(df, existing_column_name, new_column_name):
    """
    Combine byte0...byte7 columns that represent message parts to one column which directly name is message.

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame containing the byte columns.
    new_column_name : str
        Name of new column to be added.
    existing_column_name : str
        Name of the existing column containing dlc.
    Returns
    -------
    pl.DataFrame
        DataFrame with newly added message str column.
    """

    byte_columns = [f"byte_{i}" for i in range(df[existing_column_name].max())]
    missing_columns = [col for col in byte_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing byte columns: {missing_columns}")

    return df.with_columns(
        pl.concat_str(
            byte_columns,
            ignore_nulls=True,
        ).alias(new_column_name)
    )


def merge_multiple_dfs_bytes_to_message_column(
    dfs, existing_column_name, new_column_name
):
    """
    Combine byte columns from multiple DataFrames into a new message column.

    Parameters
    ----------
    dfs : list
        List of DataFrame.
    new_column_name : str
        Name of new column to be added
    existing_column_name : str
        Name of the existing column containing dlc.

    Returns
    -------
    list
        List of updated DataFrame.
    """
    return [merge_byte_columns(df, existing_column_name, new_column_name) for df in dfs]


def convert_data_types(dfs):
    """
    Convert some columns into another formats such as timestamp to datetime, string can_id in hex format into int can_id,
    bytes columns which is in string hex to int byte columns.

    Parameters
    ----------
    dfs : list
        List of DataFrame.

    Returns
    -------
    list
        List of updated DataFrames.
    """

    new_timestamp_column_name = "datetime"
    existing_timestamp_column_name = "timestamp"

    converted_timestamp_dfs = convert_multiple_dfs_timestamp_to_datetime(
        dfs, new_timestamp_column_name, existing_timestamp_column_name
    )
    new_can_id_column_name = "can_id"
    existing_can_id_column_name = "can_id"
    converted_can_id_dfs = convert_multiple_dfs_str_hex_can_id_to_int(
        converted_timestamp_dfs, new_can_id_column_name, existing_can_id_column_name
    )
    existing_dlc_column_name = "dlc"
    max_dlc_number = max([df[existing_dlc_column_name].max() for df in dfs])

    existing_byte_column_names = [f"byte_{i}" for i in range(max_dlc_number)]
    new_byte_column_names = [
        f"byte_{i}" for i in range(dos_df[existing_dlc_column_name].max())
    ]

    return convert_multiple_dfs_bytes_to_int(
        converted_can_id_dfs, existing_byte_column_names, new_byte_column_names
    )


def add_updated_flag_column_to_attack_free(df):
    """
    Adds an 'updated_flag' column to the attack-free DataFrame, assigning 'R'
    (representing a normal message) to all rows.

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame to which the 'updated_flag' column will be added.

    Returns
    -------
    pl.DataFrame
        DataFrame with newly added updated_flag str column.
    """
    updated_flag = pl.Series("updated_flag", ["R"] * len(df))
    return df.with_columns(updated_flag)


def add_features(dfs):
    """
    Add updated_flag column into Attack Free dataset as R which indicates normal message.

    Parameters
    ----------
    dfs : List
        List of DataFrame.

    Returns
    -------
    List
        List of updated DataFrame.
    """

    dos_df, fuzzy_df, attack_free_df = dfs
    attack_free_df = add_updated_flag_column_to_attack_free(attack_free_df)
    return dos_df, fuzzy_df, attack_free_df


def get_byte_column_names(dfs, dlc_column):
    """Get column list of byte columns from byte_0 to byte_7.

    Parameters
    ----------
    dfs : list
        List of DataFrame.
    dlc_column : str
        Column name which represents dlc, as dlc indicates byte column numbers such as if dlc is max 2,
        then data  have 2 columns.


    Returns
    -------
    list
        List of byte columns.
    """
    max_dlc_value = max([df[dlc_column].max() for df in dfs])
    return [f"byte_{i}" for i in range(max_dlc_value)]


def drop_features(dfs):
    """
    Drops specific features from the attack_free_df in the dataset list.

    Parameters
    ----------
    dfs : list
        A list containing three DataFrames in the following order:
        [dos_df, fuzzy_df, attack_free_df].

    Returns
    -------
    list
        A list of updated DataFrames with 'frame_type' column removed from attack_free_df.
    """
    dos_df, fuzzy_df, attack_free_df = dfs
    columns_to_delete = ["frame_type"]
    attack_free_df = drop_columns(attack_free_df, columns_to_delete, backend="polars")
    return [dos_df, fuzzy_df, attack_free_df]


def swap_features_in_specific_order(dfs, specific_order):
    """
    Reorders the features (columns) of each DataFrame in the input list to match a specific order.

    Parameters
    ----------
    dfs : list of pl.DataFrame
        A list of DataFrame objects whose columns need to be reordered.
    specific_order : list of str
        A list specifying the desired column order

    Returns
    -------
    list
        A list of DataFrames, each with columns reordered to match the `specific_order`.
    """
    return [df.select(specific_order) for df in dfs]


def encode_updated_flag_column(df, existing_flag_column_name):
    """
    Encode the updated flag column where 'R' represents normal messages (0)
    and 'T' represents injected messages (1).

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame.
    existing_flag_column_name : str
        Name of the column to be encoded.

    Returns
    -------
    pl.DataFrame
        DataFrame with the encoded flag column.
    """
    return df.with_columns(
        pl.when(pl.col(existing_flag_column_name) == "R")
        .then(0)
        .when(pl.col(existing_flag_column_name) == "T")
        .then(1)
        .otherwise(-1)  # Fallback for unexpected values
        .alias(existing_flag_column_name)
    )


def encode_multiple_dfs_updated_flag_column(dfs, existing_column_name):
    """
    Encode the updated flag column in multiple DataFrames.

    This function applies the `encode_updated_flag_column` method to a list of DataFrames,
    encoding the specified column where 'R' represents normal messages (0) and 'T'
    represents injected messages (1).

    Parameters
    ----------
    dfs : list of pl.DataFrame
        A list of input DataFrames containing the column to be encoded.
    existing_column_name : str
        The name of the column to encode in each DataFrame.

    Returns
    -------
    list of pl.DataFrame
        A list of DataFrames with the specified column encoded.
    """
    return [encode_updated_flag_column(df, existing_column_name) for df in dfs]


def encode_features(dfs):
    """
    Encode some features according to rules.

    Parameters
    ----------
    dfs : List
        List of DataFrame.

    Returns
    -------
        list of pl.DataFrame
        A list of DataFrames with the encoded columns.
    """
    existing_flag_column_name = "updated_flag"
    return encode_multiple_dfs_updated_flag_column(dfs, existing_flag_column_name)


if __name__ == "__main__":

    print("Loading dataset paths!")

    output_data_paths = load_data_paths("out_paths")
    dos_df_out_path = output_data_paths["dos_df"]
    fuzzy_df_out_path = output_data_paths["fuzzy_df"]
    attack_free_df_out_path = output_data_paths["attack_free_df"]

    print("Reading datasets!")
    dos_df, fuzzy_df, attack_free_df = read_datasets(
        [dos_df_out_path, fuzzy_df_out_path, attack_free_df_out_path], backend="polars"
    )

    print("Converting data types!")
    converted_data_types_dfs = convert_data_types([dos_df, fuzzy_df, attack_free_df])

    print("Adding new features!")
    dos_df, fuzzy_df, attack_free_df = add_features(converted_data_types_dfs)

    print("Dropping unused features!")
    dos_df, fuzzy_df, attack_free_df = drop_features([dos_df, fuzzy_df, attack_free_df])
    max_dlc_number = max([df["dlc"].max() for df in [dos_df, fuzzy_df, attack_free_df]])
    specific_order = (
        ["can_id", "timestamp", "datetime", "dlc"]
        + [f"byte_{i}" for i in range(max_dlc_number)]
        + ["updated_flag"]
    )

    print("Swapping feature orders.")
    dos_df, fuzzy_df, attack_free_df = swap_features_in_specific_order(
        [dos_df, fuzzy_df, attack_free_df],
        specific_order,
    )

    # print("Encoding features.")
    dos_df, fuzzy_df, attack_free_df = encode_features(
        [dos_df, fuzzy_df, attack_free_df]
    )

    # save_df_to_csv(dos_df, dos_df_out_path, backend="polars")
    # save_df_to_csv(fuzzy_df, fuzzy_df_out_path, backend="polars")
    # save_df_to_csv(attack_free_df, attack_free_df_out_path, backend="polars")
    # print("DataFrame Preprocessing Completed and Saved into Output Folder!")
