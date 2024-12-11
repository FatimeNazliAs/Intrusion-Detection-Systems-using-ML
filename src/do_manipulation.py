import polars as pl


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

    if existing_column_name not in df.columns:
        raise ValueError
    (f"Column '{existing_column_name}' not found in DataFrame.")

    return df.with_columns(
        pl.from_epoch(pl.col(existing_column_name), time_unit="s").alias(
            new_column_name
        )
    )


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


def convert_str_hex_to_int(df, new_column_name, existing_column_name):
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

    if existing_column_name not in df.columns:
        raise ValueError
    (f"Column '{existing_column_name}' not found in DataFrame.")

    return df.with_columns(
        pl.col(existing_column_name)
        .str.to_integer(base=16, strict=True)
        .alias(new_column_name)
    )


def convert_multiple_dfs_str_hex_canid_to_int(
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
        convert_str_hex_to_int(df, new_column_name, existing_column_name) for df in dfs
    ]


if __name__ == "__main__":
    dos_df_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/output/dos_df.csv"
    )
    fuzzy_df_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/output/fuzzy_df.csv"
    )
    attack_free_df_path = (
        r"C:/Users/Naz/Documents/GitHub/Newky/IDS/"
        r"Intrusion-Detection-Systems-using-ML/output/attack_free_df.csv"
    )

    dos_df = pl.read_csv(dos_df_path)
    fuzzy_df = pl.read_csv(fuzzy_df_path)
    attack_free_df = pl.read_csv(attack_free_df_path)

    dfs = [dos_df, fuzzy_df, attack_free_df]
    new_timestamp_column_name = "datetime"
    existing_timestamp_column_name = "timestamp"

    converted_dfs = convert_multiple_dfs_timestamp_to_datetime(
        dfs, new_timestamp_column_name, existing_timestamp_column_name
    )
    dos_df, fuzzy_df, attack_free_df = converted_dfs

    dfs = [dos_df, fuzzy_df, attack_free_df]
    new_canid_column_name =  "updatedCanIdInt"
    existing_canid_column_name = "canId"

    converted_dfs = convert_multiple_dfs_str_hex_canid_to_int(
        dfs, new_canid_column_name, existing_canid_column_name
    )
    dos_df, fuzzy_df, attack_free_df = converted_dfs
    print(dos_df.head())
    print(fuzzy_df.head())
    print(attack_free_df.head())


