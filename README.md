# Intrusion-Detection-Systems-using-ML

## 📜 Project Overview  
This project focuses on building an **Intrusion Detection System (IDS)** using machine learning techniques. The IDS analyzes automotive cybersecurity datasets to identify potential intrusions and anomalies. It emphasizes efficient preprocessing of large datasets, preparing high-quality data for downstream ML modeling.

## 🚀 Features  
- **Dual Data Processing Engines**: Supports both [Polars](https://pola-rs.github.io/polars/) and Pandas for flexible and efficient data handling.  
- **Optimized Dataset Loading**: Uses Polars for high-speed data ingestion with large files (~9 million rows).  
- **Data Preprocessing**: Clean, transform, and sample data using intuitive Pandas workflows.  
- **Exploratory Analysis**: Offers visual and statistical analysis tools via Jupyter notebooks.  
- **Modular Design**: Shared utility functions for easier maintenance and reusability.  

## 📂 Repository Structure  
```plaintext
Intrusion-Detection-Systems-using-ML/
├── input/                       # Raw dataset files from Car Hacking Dataset
│   ├── attack_free.txt
│   ├── dos_dataset.csv
│   ├── fuzzy_dataset.csv
├── output/                      # Processed datasets ready for analysis
│   ├── attack_free_df.csv
│   ├── dos_df.csv
│   ├── fuzzy_df.csv
├── notebooks/                  # Jupyter notebooks for analysis
│   ├── eda.ipynb                # Exploratory data analysis using Pandas
│   ├── preprocess_data.ipynb    # Data cleaning and transformation
│   ├── solve_dlc_flag_issue.ipynb # Fixing misplaced DLC/flag column
│   ├── visualize_data.ipynb     # Data visualization with charts
│   ├── utils.ipynb              # Helper functions for notebooks
├── src/                         # Python scripts for production-ready data processing
│   ├── load_data_with_polars.py         # 🚀 Actively used: Efficient loading using Polars
│   ├── preprocess_data_with_pandas.py   # ✅ Actively used: Sampling & cleaning using Pandas
│   ├── utils.py                         # ✅ Actively used: Shared helper functions
│   ├── load_data_with_pandas.py         # ⚠️ Not used (slow on large data, kept for reference)
│   ├── preprocess_data_with_polars.py   # ⚠️ Not used (replaced with Pandas version)
│   ├── train_model.py                   # ML model training (coming soon)
├── README.md                  # Project documentation

```
---

### ✅ Currently Used Code Files

| **File**                       | **Purpose**                                                        |
|---------------------------------|--------------------------------------------------------------------|
| `load_data_with_polars.py`      | Loads full datasets efficiently using Polars                       |
| `preprocess_data_with_pandas.py`| Preprocesses sampled data using Pandas, suitable for ML workflows  |
| `utils.py`                      | Stores common helper functions used across both engines            |


### ❌ Deprecated / Reference Files

| **File**                        | **Notes**                                                                   |
|----------------------------------|-----------------------------------------------------------------------------|
| `load_data_with_pandas.py`       | Legacy loader; not recommended for large-scale data loading                 |
| `preprocess_data_with_polars.py` | Old preprocessing logic; replaced for better maintainability with Pandas    |

---

### 🧠 Why Both Pandas & Polars?
- **Polars** is preferred for *initial full data loading* due to its speed and memory efficiency.
- **Pandas** is used for *preprocessing sampled data*—it's more intuitive and integrates well with visualization and ML tools.


## 📊 Datasets  
The raw datasets are taken from the **Car Hacking Dataset**, which contains records for intrusion detection, such as:  
- `attack_free.txt`: Attack-free dataset.  
- `dos_dataset.csv`: Denial of Service (DoS) dataset.  
- `fuzzy_dataset.csv`: Fuzzy intrusion dataset.

Processed datasets are saved in the `output` folder as:  
- `attack_free_df.csv`  
- `dos_df.csv`  
- `fuzzy_df.csv`  

## 🛠️ Setup Instructions  

1. Clone the repository:  
   ```bash
   git clone https://github.com/yourusername/Intrusion-Detection-Systems-using-ML.git
   cd Intrusion-Detection-Systems-using-ML
2. Install dependencies:  
   Create and activate a virtual environment, then install requirements:  
   ```bash
   python -m venv venv
   source venv/bin/activate   # For Linux/Mac
   venv\Scripts\activate      # For Windows
   pip install -r requirements.txt
3. Run preprocessing scripts:
   Use the src scripts to generate processed datasets:
   ```bash
   python src/load_data_with_polars.py
   python src/preprocess_data_with_pandas.py

### 📝 Usage
- **Load Full Dataset**: Use `src/load_data_with_polars.py` for quick ingestion of large files.
- **Preprocess Data**: Run `src/preprocess_data_with_pandas.py` after sampling for manageable processing.
- **Explore Data**: Open `notebooks/eda.ipynb` for insights into distributions, anomalies, and patterns.
- **Visualize Data**: Generate visual summaries using `notebooks/visualize_data.ipynb`.

---

### 📜 License
This project is licensed under the MIT License.

---

### 🙌 Acknowledgments
- **Car Hacking Dataset** — source of real CAN bus data.
- **Polars** — for blazing-fast data loading.
- **Open-source community** — for tools and guidance that power this project.
