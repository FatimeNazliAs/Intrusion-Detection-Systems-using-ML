from utils import load_data_paths_from_config, load_datasets


if __name__ == "__main__":
    print("Loading dataset paths.")
    dos_df_out_path, fuzzy_df_out_path, attack_free_csv_out_path = (
        load_data_paths_from_config("out_paths")
    )
    print("Loading datasets.")

    dos_df, fuzzy_df, attack_free_df = load_datasets(
        dos_df_out_path, fuzzy_df_out_path, attack_free_csv_out_path
    )
    print(dos_df.dtypes)
