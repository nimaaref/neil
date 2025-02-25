# Neil: NFL Prediction Model (WIP)

Neil is a machine learning-based **NFL game prediction model** that utilizes **random forest classification** and **weekly NFL data** to predict game outcomes. This project integrates **data import, transformation, feature engineering, and modeling** into an automated pipeline.

## Features

 **Automated Data Pipeline**: Imports weekly **NFL data & schedules** from `nfl_data_py` and stores it in **SQLite**.
 **Feature Engineering**: Calculates key stats like **total yards, turnovers, and 2pt conversions**.
**Machine Learning Model**: Uses **Random Forest** to train and predict game outcomes.
**Configurable & Scalable**: Supports **different seasons, feature toggling, and prediction weeks**.
**Modular Structure**: Organized into separate **data import, transformation, and model training** steps.

## Project Structure

```
📦 neil
 ┣ 📂 data_import           # Scripts to fetch and store data
 ┣ 📂 data_transform        # Scripts to clean & transform data
 ┣ 📂 models               # Machine learning model training & predictions
 ┣ 📂 feature_engineering   # Feature selection & engineering logic
 ┣ 📂 utils                # Helper functions
 ┣ 📜 config.py            # Configuration settings (database, API, season settings)
 ┣ 📜 README.md            # Project documentation (you're reading this!)
 ┣ 📜 requirements.txt      # Python dependencies
 ┗ 📜 .gitignore           # Files to ignore in Git
```

## Installation & Setup

1. **Clone the Repository**
```sh
 git clone https://github.com/nimaaref/neil.git
 cd neil
```

2. **Set Up a Virtual Environment**
```sh
 python -m venv venv  # Create virtual env
 source venv/bin/activate  # Mac/Linux
 venv\Scripts\activate  # Windows
```

3. **Install Dependencies**
```sh
pip install -r requirements.txt
```

4. **Configure Database** (Optional: Drop existing tables)
```sh
sqlite3 nfl_data.db "DROP TABLE IF EXISTS weekly_nfl_data;"
```

5. **Run Data Import & Transformations**
```sh
python data_import/import_weekly_data.py
python data_transform/transform_schedules.py
```

6. **Train & Predict**
```sh
python models/train_model.py
python models/predict.py
```

## Development & Debugging

- Run **VS Code Debugger** with `.vscode/launch.json` configured.

## Roadmap & TODOs

- **Improve Feature Engineering**: Add more advanced metrics.
- **Optimize Model Performance**: Tune hyperparameters & test different ML models.
- **Automate Data Refresh**: Set up cron jobs for weekly updates.
- **Expand Predictions**: Support player-based and advanced analytics.

## License

This project is licensed under **[Apache License](LICENSE)**. Feel free to use and modify it, but please give credit!

---

**Contributions & Feedback Welcome!** This is an ongoing project, and I’d love any suggestions or improvements. Feel free to open an **issue** or **pull request**!

