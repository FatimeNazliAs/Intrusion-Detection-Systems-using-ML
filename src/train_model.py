from utils import (
    load_datasets,
    drop_columns,
    return_non_attack_df,
    return_only_attack_df,
    add_and_fill_column,
)
import polars as pl
from sklearn.model_selection import StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score


def prepare_data_for_modelling(updated_flag_column):

    updated_flag_column = "updatedFlag"

    only_dos_df = return_only_attack_df(dos_df, updated_flag_column)
    only_fuzzy_df = return_only_attack_df(fuzzy_df, updated_flag_column)

    attack_free_in_dos = return_non_attack_df(dos_df, updated_flag_column)
    attack_free_in_fuzzy = return_non_attack_df(fuzzy_df, updated_flag_column)

    all_attack_free_df = pl.concat(
        [attack_free_in_fuzzy, attack_free_in_dos, attack_free_df]
    )

    columns_to_delete = ["datetime", "updatedFlag"]
    only_dos_df = drop_columns(only_dos_df, columns_to_delete)
    only_fuzzy_df = drop_columns(only_fuzzy_df, columns_to_delete)
    all_attack_free_df = drop_columns(all_attack_free_df, columns_to_delete)

    only_dos_df = add_and_fill_column(only_dos_df, "attackType", 1)
    only_fuzzy_df = add_and_fill_column(only_fuzzy_df, "attackType", 2)
    all_attack_free_df = add_and_fill_column(all_attack_free_df, "attackType", 0)

    all_attack_free_df = all_attack_free_df.filter(pl.col("dlc") == 8)

    return only_dos_df, only_fuzzy_df, all_attack_free_df


if __name__ == "__main__":
    dos_df, fuzzy_df, attack_free_df = load_datasets("out_paths")

    updated_flag_column = "updatedFlag"
    dos_df, fuzzy_df, attack_free_df = prepare_data_for_modelling(updated_flag_column)
    df = pl.concat([fuzzy_df, dos_df, attack_free_df])
    print(dos_df.head())

    X = df.drop(["attackType"])
    y = df["attackType"]
    skf = StratifiedKFold(n_splits=5)

    for train_index, test_index in skf.split(X, y):
        X_train, X_val = X[train_index], X[test_index]
        y_train, y_val = y[train_index], y[test_index]

    models = {
        # "Logistic Regression": LogisticRegression(),
        # "Decision Tree": DecisionTreeClassifier(),
        # "Random Forest": RandomForestClassifier(),
        "SVC": SVC(),
        "KNN": KNeighborsClassifier(),
    }
    best_acc = float("inf")
    best_model = None

    # for model_name, model in models.items():
    #     print(f"Training {model_name}...")
    #     model.fit(X_train, y_train)

    #     # Predict on validation set
    #     val_predictions = model.predict(X_val)
    #     val_acc = accuracy_score(y_val, val_predictions)
    #     print(f"{model_name} Accuracy Score: {val_acc}")

    #     # Track the best model
    #     if val_acc < best_acc:
    #         best_acc = val_acc
    #         best_model = model

    # print(best_model)