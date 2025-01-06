from omegaconf import OmegaConf
import os
import polars as pl


def load_data_paths(path_name):
    dir = os.getcwd()
    config_path = os.path.join(dir, "config.yaml")
    config = OmegaConf.load(config_path)
    paths = config[path_name]
    dos_df_path = os.path.join(dir, paths.dos_df).replace("\\", "/")
    fuzzy_df_path = os.path.join(dir, paths.fuzzy_df).replace("\\", "/")
    attack_free_df_path = os.path.join(dir, paths.attack_free_df).replace("\\", "/")
    return dos_df_path, fuzzy_df_path, attack_free_df_path


def load_datasets(path_name):
    dos_df_path, fuzzy_df_path, attack_free_df_path = load_data_paths(path_name)
    dos_df = pl.read_csv(dos_df_path)
    fuzzy_df = pl.read_csv(fuzzy_df_path)
    attack_free_df = pl.read_csv(attack_free_df_path)
    return dos_df, fuzzy_df, attack_free_df


def drop_columns(df, columns_to_delete):
    """
    Drop multiple columns from DataFrame

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame to which the columns_to_delete will be deleted.

    Returns
    -------
    pl.DataFrame
        DataFrame with newly deleted columns.
    """
    return df.drop(columns_to_delete)


def add_and_fill_column(df, column_to_add, fill_value):
    return df.with_columns(pl.lit(fill_value).alias(column_to_add))


def return_non_attack_df(df, column_name):
    return df.filter(pl.col(column_name) == 0)


def return_only_attack_df(df, column_name):
    return df.filter(pl.col(column_name) == 1)
