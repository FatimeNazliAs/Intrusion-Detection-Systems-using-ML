# Intrusion-Detection-Systems-using-ML

## 📜 Project Overview  
This project focuses on developing an **Intrusion Detection System (IDS)** using machine learning techniques. The IDS aims to analyze datasets for intrusion patterns, preprocess data efficiently, and prepare the foundation for applying machine learning models.

## 🚀 Features  
- **Dataset Preprocessing**: Handles large datasets efficiently using [Polars](https://pola-rs.github.io/polars/).  
- **Data Exploration**: Detailed exploration and analysis of datasets.  
- **Data Manipulation**: Handles data cleaning, feature engineering, and processing.  
- **Performance Optimization**: Optimized processing of large datasets compared to traditional libraries like Pandas.  

## 📂 Repository Structure  
```plaintext
Intrusion-Detection-Systems-using-ML/
├── input/                   # Raw dataset files from Car Hacking Dataset
│   ├── attack_free.txt
│   ├── dos_dataset.csv
│   ├── fuzzy_dataset.csv
├── output/                  # Processed datasets for analysis
│   ├── attack_free_df.csv
│   ├── dos_df.csv
│   ├── fuzzy_df.csv
├── notebooks/                   # Jupyter notebooks for testing and analysis
│   ├── eda.ipynb                # Exploratory data analysis using Pandas
│   ├── preprocess_data.ipynb    # Data manipulation and feature engineering
│   ├── fix_data_issue.ipynb     # Solving misplaced dlc-flag column issue 
│   ├── visualize_data.ipynb     # Data visualization with graphics
│   ├── utils.ipynb              # Some helper functions
├── src/                         # Final Python scripts for processing data
│   ├── get_data_pandas.py       # Dataset loading with Pandas
│   ├── load_data.py             # Optimized dataset loading with Polars
│   ├── preprocess_data.py       # Data manipulation functions
│   ├── train_model.py           # Data modelling
│   ├── utils.py                 # Helper Functions
├── README.md                # Project documentation (you're reading this)
```

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
   python src/load_data.py

   
## 📝 Usage
- **Load data**: Process large datasets with optimized Polars methods in src/load_data.py.
- **Exploration**: Use notebooks/eda.ipynb to explore datasets.
- **Manipulation**: Modify datasets using notebooks/preprocess_data.ipynb or src/preprocess_data.py.
- **Visualization**: Visualize datasets using notebooks/visualize_data.ipynb
  
## 🤝 Contributions
Contributions are welcome! Please fork the repository, make your changes, and submit a pull request.

## 📜 License
- This project is licensed under the MIT License.

## 🙌 Acknowledgments
- **Car Hacking Dataset**: The source of the raw datasets.
- **Polars Library**: For efficient data manipulation.
- **Open-source community** for inspiration and tools.


