import os
import concurrent.futures
import argparse

def split_large_csv_chunk(start, end, input_file, output_directory):
    output_file = f"{output_directory}/output_{start}_{end}.csv"
    with open(input_file, 'r', encoding='utf-8') as infile:
        # header = infile.readline()  # Read the header
        with open(output_file, 'w', encoding='utf8') as outfile:
            # outfile.write(header)  # Write the header to each output file
            line_count = 0
            for idx, line in enumerate(infile):
                if start <= idx < end:
                    outfile.write(line)
                    line_count += 1
            print(f"Processed lines {start}-{end} and saved to {output_file}, Total lines: {line_count}")

def split_large_csv_multithreaded(input_file, output_directory, num_threads):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    with open(input_file, 'r', encoding='utf-8') as infile:
        total_lines = sum(1 for _ in infile)

    lines_per_thread = total_lines // num_threads

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            start = i * lines_per_thread
            end = (i + 1) * lines_per_thread if i < num_threads - 1 else total_lines
            futures.append(executor.submit(split_large_csv_chunk, start, end, input_file, output_directory))

        concurrent.futures.wait(futures)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Split a large CSV file into smaller chunks using multithreading.')
    parser.add_argument('input_file', type=str, help='Path to the input CSV file')
    parser.add_argument('output_directory', type=str, help='Directory to save the output CSV files')
    parser.add_argument('num_threads', type=int, help='Number of threads for parallel processing')

    args = parser.parse_args()
    input_file = args.input_file
    output_directory = args.output_directory
    num_threads = args.num_threads

    split_large_csv_multithreaded(input_file, output_directory, num_threads)