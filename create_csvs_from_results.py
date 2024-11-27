import pandas as pd
import os

yearly_counts_columns = [
    'year', 'state', 'accident_count'
]

weather_time_columns = [
    'time_of_day', 'weather_category', 'accident_count', 
    'avg_severity', 'avg_temp', 'avg_visibility', 
    'avg_wind_speed', 'avg_precipitation'
]

severity_patterns_states_columns = [
    'state', 'total_accidents', 'severe_accidents',
    'avg_severity_score', 'severe_accident_percentage'
]

monthly_counts_columns = [
    'month', 'accident_count'
]

def process_files(input_dir, output_file, column_names):
    if os.path.exists(output_file):
        print(f"The CSV file already exists: {output_file}")
        return
    
    files = [f for f in os.listdir(input_dir) if not f.startswith('.')]
    
    if not files:
        print(f"No files found in {input_dir}")
        
    complete_df = pd.DataFrame()
    
    for file in files:
        file_path = os.path.join(input_dir, file)
        temp_df = pd.read_csv(file_path, header=None, names=column_names)
        complete_df = pd.concat([complete_df, temp_df], ignore_index=True)
    
    complete_df.to_csv(output_file, index=False)
    print(f"File: {output_file} created successfully!")
    
process_files(
    input_dir='results/yearly_counts',
    output_file='results/yearly_counts/yearly_counts_analysis.csv',
    column_names=yearly_counts_columns
)

process_files(
    input_dir='results/weather_time_analysis',
    output_file='results/weather_time_analysis/weather_time_analysis.csv',
    column_names=weather_time_columns
)

process_files(
    input_dir='results/severity_patterns_states',
    output_file='results/severity_patterns_states/severity_patterns_states_analysis.csv',
    column_names=severity_patterns_states_columns
)

process_files(
    input_dir='results/monthly_counts',
    output_file='results/monthly_counts/monthly_counts.csv',
    column_names=monthly_counts_columns
)

