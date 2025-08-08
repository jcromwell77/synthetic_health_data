'''
This is the parent program that controls the child programs: generate_patient_data and generate_health_data.
The intent of the programs is to create synthetic data. It will create a single patient file. The patient
data will then be used to generate daily health data for each patient for a start date and end date.
'''
from generate_patient_data import generate_patients
from generate_health_data import generate_health

# Variables to be updated by the user before running.
patients_to_create=10000
health_start_year_to_create=2014
health_end_year_to_create=2024

# The main function that is used to pass the variables to the child programs.
def main():
    patients =  generate_patients(num_patients=patients_to_create)
    print(f"Patient List Size: {len(patients)}")
    generate_health(patients, start_year=health_start_year_to_create, end_year=health_end_year_to_create)

# Call to main()
if __name__ == "__main__":
    main()