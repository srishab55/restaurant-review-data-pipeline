import json
import os

def split_json(input_file, output_dir, lines_per_file=50):
    """
    Splits a large JSON file (assumed to be a dictionary of objects) into smaller files with a specified number of items per file.

    Args:
    - input_file (str): Path to the large JSON file.
    - output_dir (str): Directory to save the split files.
    - lines_per_file (int): Number of items per chunk file.
    """
    # Read the large JSON file
    with open(input_file, 'r') as infile:
        # Load the JSON data, assuming it's a dictionary of restaurants
        data = json.load(infile)

    print(f"Loaded JSON data from {input_file}")
    # Check if data is a dictionary (as expected)
    if not isinstance(data, dict):
        print("Error: Expected JSON data to be a dictionary of objects.")
        return

    # Convert the dictionary to a list of restaurant entries
    restaurant_list = [{"id": key, **value} for key, value in data.items()]

    # Determine how many chunks we need
    total_lines = len(restaurant_list)
    num_chunks = (total_lines // lines_per_file) + (1 if total_lines % lines_per_file else 0)
    
    print(f"Total entries in JSON: {total_lines}")
    print(f"Splitting into {num_chunks} files, each containing up to {lines_per_file} items.")

    # Split the data into chunks and write them to new files
    for i in range(num_chunks):
        start_idx = i * lines_per_file
        end_idx = (i + 1) * lines_per_file
        chunk_data = restaurant_list[start_idx:end_idx]

        # Generate the output filename
        output_file = f"{output_dir}/chunk_{i+1}.json"
        
        # Write the chunk to a new file
        with open(output_file, 'w') as outfile:
            json.dump(chunk_data, outfile, indent=4)
        print(f"Written chunk {i+1} to {output_file}")

if __name__ == "__main__":
    input_file = "data/raw/restaurants.json"  # Path to your large JSON file
    output_dir = "data/interim/restaurants"  # Directory where the smaller JSON files will be saved
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Split the JSON file
    split_json(input_file, output_dir)