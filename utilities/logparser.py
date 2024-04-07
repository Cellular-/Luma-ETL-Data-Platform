from pathlib import Path
import sys, argparse

def get_filenames(dirname, filename_pattern):
    files = Path(dirname).glob(filename_pattern)
    return [file for file in files]

def read_lines(filename, encoding):
    with open(filename, 'r', encoding=encoding) as f:
        for line in f:
            yield line

def bom_file_to_utf_8(filename):
    bom_lines = read_lines(filename, encoding='utf-16')
    with open(f'{filename}.conv', 'w', encoding='utf-8') as fn:
        for line in bom_lines:
            fn.write(line)

def target_bc(line):    
    return 'Target Business Class' in line

def extract_duration(line):
    return 'Infor Extraction Duration' in line

def stage_load_duration(line):    
    return 'Staging Load Duration' in line

def extract_log_stats(filename):
    lines = []
    for line in read_lines(filename, encoding='utf-8'):
        if extract_duration(line): 
            lines.append(line.split(' ')[-1])
        elif target_bc(line): 
            extract_dur_line = lines.pop()
            lines.append(line.split(' ')[-1])
            lines.append(extract_dur_line.split(' ')[-1])
        elif stage_load_duration(line): lines.append(line.split(' ')[-1])
        else: continue

    return lines

def log_summary_stats(filename):
    result = []
    current_list = None

    for line in read_lines(filename, 'utf-8'):
        if line == '\n' or '_run.log' in line:
            continue
        elif 'ERRORS' in line:
            break        
        elif line.startswith(">>>"):
            if current_list:
                result.append(current_list)
            current_list = [line]
        elif current_list:
            current_list.append(line)

    # Add the last list if it exists
    if current_list:
        result.append(current_list)

    return result

            
if __name__ == '__main__':
    # Create an argument parser
    parser = argparse.ArgumentParser(description="A Python script to parse directory_name, filename_pattern, and output_filename.")

    # Add the command-line arguments
    parser.add_argument("directory_name", type=str, help="Directory to search for files.")
    parser.add_argument("filename_pattern", type=str, help="Pattern to match filenames in the directory.")
    parser.add_argument("output_filename", type=str, help="Name of the output file.")

    # Parse the command-line arguments
    args = parser.parse_args()    
    filenames = get_filenames(args.directory_name, args.filename_pattern)

    with open(args.output_filename, 'w') as f:
        for filename in filenames:
            try:
                converted_filename = None
                bom_file_to_utf_8(filename)
                converted_filename = f'{filename}.conv'
            except UnicodeError as e:
                print(e)
            
            data = log_summary_stats(converted_filename or filename)
            f.write(
                ''.join([f'{row[0].strip()},{row[1].strip()}' for row in data])
            )