from omegaconf import OmegaConf
import os


def load_data_paths_from_config(path_name):
    dir = os.getcwd()
    config_path = os.path.join(dir, "config.yaml")
    config = OmegaConf.load(config_path)
    paths = config[path_name]
    dos_df_path = os.path.join(dir, paths.dos_df).replace("\\", "/")
    fuzzy_df_path = os.path.join(dir, paths.fuzzy_df).replace("\\", "/")
    attack_free_df_path = os.path.join(dir, paths.attack_free_df).replace("\\", "/")
    # print(dos_df_path)
    # print(fuzzy_df_path)
    return dos_df_path, fuzzy_df_path, attack_free_df_path



