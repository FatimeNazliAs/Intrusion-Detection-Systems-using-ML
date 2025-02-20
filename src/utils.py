from omegaconf import OmegaConf
import os
import polars as pl


def load_data_paths(path_type):
    """Load dataset paths from config.yaml based on the given path type.

    Parameters
    ----------
    path_type : str
        Either 'in_paths' for input files or 'out_paths' for output files.

    Returns
    -------
    dict
        a dictionary where each key is dataset name, each value is its full file path.
    Raises
    ------
    ValueError
        If 'path_type' is not found in the config file.
    """
    dir = os.getcwd()
    config_path = os.path.join(dir, "config.yaml")
    config = OmegaConf.load(config_path)
    if path_type not in config:
        raise ValueError(
            f"Invalid path_type '{path_type}'. Choose 'in_paths' or 'out_paths'."
        )

    paths_dict = config[path_type]
    data_paths_dict = {
        key: os.path.join(dir, value).replace("\\", "/")
        for key, value in paths_dict.items()
    }
    return data_paths_dict


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


def save_pl_df_to_csv(df: pl.DataFrame, df_path: str):
    """
    Save a Polars DataFrame to a CSV file.

    Parameters
    ----------
    df : pl.DataFrame
        The Polars DataFrame to be saved.

    df_path : str
        The file path where the DataFrame will be saved.
    """
    try:
        df.write_csv(df_path)
    except Exception as e:
        print(f"Error: Could not save DataFrame to {df_path}. Exception: {e}")


def save_pd_df_to_csv(df, df_path):
    """
    Save a Pandas DataFrame to a specified output folder

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be saved

    df_path :str
        Path to save the DataFrame
    """
    try:
        df.to_csv(df_path)
    except Exception as e:
        print(f"Failed to save DataFrame to {df_path}: {e}")


# def load_datasets(path_name):
#     dos_df_path, fuzzy_df_path, attack_free_df_path = load_data_paths(path_name)
#     dos_df = pl.read_csv(dos_df_path)
#     fuzzy_df = pl.read_csv(fuzzy_df_path)
#     attack_free_df = pl.read_csv(attack_free_df_path)
#     return dos_df, fuzzy_df, attack_free_df


# def drop_columns(df, columns_to_delete):
#     """
#     Drop multiple columns from DataFrame

#     Parameters
#     ----------
#     df : pl.DataFrame
#         The input DataFrame to which the columns_to_delete will be deleted.

#     Returns
#     -------
#     pl.DataFrame
#         DataFrame with newly deleted columns.
#     """
#     return df.drop(columns_to_delete)


# def add_and_fill_column(df, column_to_add, fill_value):
#     return df.with_columns(pl.lit(fill_value).alias(column_to_add))


# def return_non_attack_df(df, column_name):
#     return df.filter(pl.col(column_name) == 0)


# def return_only_attack_df(df, column_name):
#     return df.filter(pl.col(column_name) == 1)
