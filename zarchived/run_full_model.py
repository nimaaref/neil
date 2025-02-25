import os
import subprocess
import logging
from datetime import datetime

def setup_logging():
    """Set up logging configuration"""
    log_filename = f"script_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

def run_python_files(directory='.'):
    """
    Run specified Python files in the given order
    
    Args:
        directory (str): Directory containing the Python files
    """
    # Define the order of execution (add your filenames here)
    execution_order = [
        'import_weekly_data.py',
        'import_weekly_schedules.py',
        'trans_weekly_home_away.py',
        'train_model.py',
        'predict_outcomes.py'
    ]
    
    logging.info(f"Starting execution of {len(execution_order)} Python files")
    
    for file in execution_order:
        file_path = os.path.join(directory, file)
        
        # Check if file exists
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file}")
            raise FileNotFoundError(f"Could not find {file} in {directory}")
            
        try:
            logging.info(f"Running {file}")
            result = subprocess.run(['python', file_path], 
                                 capture_output=True, 
                                 text=True, 
                                 check=True)
            logging.info(f"Successfully completed {file}")
            
            # Log any output from the script
            if result.stdout:
                logging.info(f"Output from {file}:\n{result.stdout}")
                
        except subprocess.CalledProcessError as e:
            logging.error(f"Error running {file}: {str(e)}")
            logging.error(f"Error output:\n{e.stderr}")
            raise Exception(f"Script execution failed at {file}")

if __name__ == "__main__":
    setup_logging()
    try:
        run_python_files()
        logging.info("All scripts completed successfully")
    except Exception as e:
        logging.error(f"Batch execution failed: {str(e)}")
        raise