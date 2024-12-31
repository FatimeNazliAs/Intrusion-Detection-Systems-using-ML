from omegaconf import OmegaConf
import os
import polars as pl
import pandas as pd


def load_data_paths(path_name):
    dir = os.getcwd()
    config_path = os.path.join(dir, "config.yaml")
    config = OmegaConf.load(config_path)
    paths = config[path_name]
    dos_df_path = os.path.join(dir, paths.dos_df).replace("\\", "/")
    fuzzy_df_path = os.path.join(dir, paths.fuzzy_df).replace("\\", "/")
    attack_free_df_path = os.path.join(dir, paths.attack_free_df).replace("\\", "/")
    return dos_df_path, fuzzy_df_path, attack_free_df_path


def load_datasets_with_pl():
    dos_df_path, fuzzy_df_path, attack_free_df_path = load_data_paths("out_paths")
    dos_df = pl.read_csv(dos_df_path)
    fuzzy_df = pl.read_csv(fuzzy_df_path)
    attack_free_df = pl.read_csv(attack_free_df_path)
    return dos_df, fuzzy_df, attack_free_df



