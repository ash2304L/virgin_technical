#This file is to make unzip and print the final output of task 1 for verification purposes.

import gzip
import json

def read_and_print_output(file_path):
    with gzip.open(file_path, 'rt') as f:
        print(f)
        for line in f:
            print(line.strip())
            #print("hello")

# Specify the path to the output file generated by the pipeline
output_file_path = 'output/results.jsonl.gz'

# Call the function to read and print the output
read_and_print_output(output_file_path)

#unzipped_file = gzip.open(output_file_path, 'rt')
#print(unzipped_file)