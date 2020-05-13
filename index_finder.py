import pandas as pd
import os
import glob
import sys


def read_files(input_path):
    file_path_list = glob.glob(input_path + "/*.csv")
    if (len(file_path_list) < 1):
        raise Exception('No files found in the input directory.')
    main_dataframe = pd.DataFrame()
    for file_path in file_path_list:
        current_df = pd.read_csv(file_path)
        main_dataframe = pd.concat([main_dataframe, current_df])

    main_dataframe['Acquired Time'] = pd.to_datetime(main_dataframe['Acquired Time'], format='%Y-%m-%d::%H:%M:%S.%f')
    main_dataframe['Clinical Time'] = pd.to_datetime(main_dataframe['Clinical Time'], format='%Y-%m-%d::%H:%M:%S.%f')
    return main_dataframe


def merge_data(main_dataframe, parameters):
    time = []
    parameter_dict = {}
    parameter_dict['time'] = []
    row_mapping = {
        'C1': 'C1 HR',
        'C2': 'C2 HR',
        'MHR': 'MHR ',
        'TOCO': 'TOCO '
    }

    for parameter in parameters:
        parameter_dict[parameter] = []

    for index, row in main_dataframe.sort_values(by='Acquired Time').iterrows():
        parameter_dict['time'].append(row['Acquired Time'])
        parameter_dict['time'].append(row['Acquired Time'])
        parameter_dict['time'].append(row['Acquired Time'])
        parameter_dict['time'].append(row['Acquired Time'])

        for parameter in parameters:
            parameter_dict[parameter].append(row[row_mapping[parameter] + '0'])
            parameter_dict[parameter].append(row[row_mapping[parameter] + '1'])
            parameter_dict[parameter].append(row[row_mapping[parameter] + '2'])
            parameter_dict[parameter].append(row[row_mapping[parameter] + '3'])

    df = pd.DataFrame(parameter_dict)

    for parameter in df.columns[1:]:
        if (df[df[parameter] > 0].shape[0] < 1):
            print('Column : ', parameter, ' is all zeroes. Dropping from main dataframe.')
            df = df.drop([parameter], axis=1)

    return df


def find_range(merged_df, top_ranges):
    index_list = []
    start_index = -1
    prev = False
    merged_df['all_non_zero'] = (merged_df.iloc[:, 1:] != 0).sum(1) == (merged_df.shape[1] - 1)

    for index, row in merged_df.iterrows():
        if (row['all_non_zero'] and not prev):
            start_index = index
            prev = True
        elif (not row['all_non_zero'] and prev):
            index_list.append((start_index, index, index - start_index + 1))
            prev = False

    if (prev):
        index_list.append((start_index, merged_df.shape[0] - 1, merged_df.shape[0] - start_index))

    index_df = pd.DataFrame(index_list, columns=['start_index', 'end_index', 'length'])
    return merged_df, index_df


input_folder_path = './input_data'
output_folder_path = './output_data'

parameter_mapping = {
    'C1': 'C1 HR',
    'C2': 'C2 HR',
    'MHR': 'MHR ',
    'TOCO': 'TOCO '
}

if (len(sys.argv) < 2):
    print('Please enter parameters in space separated format. Ex: index_finder.py C1 MHR')

parameters = sys.argv[1:]
for parameter in parameters:
    if (parameter not in parameter_mapping):
        print('Please enter parameter names in correct format. Ex: C1 C2 MHR FHR')
        sys.exit(-1)

try:
    print('Reading input data files.')
    main_df = read_files(input_folder_path)
    print('Merging input files to a single dataframe.')
    merged_df = merge_data(main_df, parameters)
    print('Identifying index range.')
    modified_df, index_df = find_range(merged_df, 5)
    print('Saving output data.')
    modified_df.to_csv(os.path.join(output_folder_path, 'output_dataframe.csv'), index=False)
    index_df.to_csv(os.path.join(output_folder_path, 'index_dataframe.csv'), index=False)
    print('Files successfully saved to output_data directory.')
except Exception as e:
    print('An error occurred while processing your data.')
    print(e)
