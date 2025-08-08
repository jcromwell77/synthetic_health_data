'''
This is the first child program called by the main program. The purpose of this program is to create
synthetic patient data. It uses the faker library to generate the synthetic data. In its current form
the data generates a random, unique patient id, and a few general starting vitals. The DataFrame that
is generated is then stored in a CSV. The CSV is used by the next program (generate_health_data).
'''
from faker import Faker
import pandas as pd

# Specifies the filepath and filename for the patient file. Recommend using this to avoid changing other parameters.
sfile = 'patient_data/patients.csv'

'''The funtion to generate the synthetic patient data. It uses faker to create the data, and can be
reconfigured to accommodate a different set up patient data as desired. However, you will need to
make sure that a patient id is created since it will be required by the generate_health_data to
create the synthetic health data.'''
def generate_patients(num_patients):
    f = Faker()
    patients = []

    # Creates the patient data to include the id. This can be changed, but it is recommended to keep 'id'
    for _ in range(num_patients):
        patient = {
            'id':f.bothify(text=f.last_name()[:2].upper()+'#####?'),
            'birthdate':f.date_of_birth(maximum_age=105, minimum_age=25),
            'gender':f.passport_gender(),
            'city':f.city(),
            'state':f.state(),
            'height':f.random_int(min=60, max=76),
            'start_weight':f.random_int(min=125, max=300),
            'start_systolic':f.random_int(min=100, max=180),
            'start_diastolic':f.random_int(min=60, max=110),
            'create_date':'2014-01-01',
            'updated_date':'2014-01-01'
        }
        patients.append(patient)

    # ✅ Confirm list length before saving
    print(f"Generated {len(patients)} patients.")

    # Save the DataFrame as a CSV to the sfile variable
    df = pd.DataFrame(patients)
    df.to_csv(sfile, index=False)

    return patients