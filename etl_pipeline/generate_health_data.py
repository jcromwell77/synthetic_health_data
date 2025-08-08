'''
This the second child program to be called from the main program. It creates synthetic health data for
patient id created in the patient.csv file. The intent was create a buld of data for various sizes to 
building test/sample projects. Therefore, it creates data for each patient on a daily basis for every 
year set in the main program. The data will partitioned by year/month. 
'''
import pandas as pd
import calendar
from datetime import date
import random
from faker import Faker
import os

f = Faker()

# This function generates the synthetic health data
def generate_health(patients, start_year, end_year):
    # Initialize current weight for each patient using start_weight
    current_weights = {patient['id']: patient['start_weight'] for patient in patients}

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            records = []
            num_days = calendar.monthrange(year, month)[1]
            for day in range(1, num_days + 1):
                current_date = date(year, month, day)
                for patient in patients:
                    pid = patient['id']
                    prev_weight = current_weights[pid]

                    # Apply random percent change between -2% and +2%
                    percent_change = random.uniform(-0.02, 0.02)
                    new_weight = round(prev_weight * (1 + percent_change), 1)

                    # Update the current weight for next day
                    current_weights[pid] = new_weight

                    # This specifies the patient data that is created on a daily basis.
                    record = {
                        'patient_id': pid,
                        'heart_rate': f.random_int(min=60, max=100),
                        'weight': new_weight,
                        'systolic': f.random_int(min=100, max=180),
                        'diastolic': f.random_int(min=60, max=110),
                        'record_date': current_date
                    }
                    records.append(record)

            # Save monthly health data to year-based folder
            folder_path = os.path.join("health_data", str(year))
            os.makedirs(folder_path, exist_ok=True)
            filename = f"health_data_{year}{month:02d}.csv"
            file_path = os.path.join(folder_path, filename)
            df = pd.DataFrame(records)
            df.to_csv(file_path, index=False)
            print(f"Saved: {file_path}")
