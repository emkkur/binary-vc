import os
import shutil
import csv
from modules.cdc import rabin_karp_cdc_stream_with_bounds
from modules.delta import Delta

# Paths to your two images
file1_path = "/Users/emilkurian/Desktop/development/binary-vc/samples/original.jpg"
file2_path = "/Users/emilkurian/Desktop/development/binary-vc/samples/modified.jpg"

# Temporary folders
cdc_dir_1 = "output/temp_chunks_1"
cdc_dir_2 = "output/temp_chunks_2"
delta_dir = "output/deltas"
# CSV output file
csv_file = "chunking_experiment_results.csv"

# Function to extract hashes from filenames
def extract_hashes(directory):
    return sorted(f.split('.')[0] for f in os.listdir(directory) if f.endswith('.bin'))

# Storage for previous run's hashes
previous_hashes_1 = None
previous_hashes_2 = None

# Set up CSV writing
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write header row
    writer.writerow([
        "Run",
        "File1_NumChunks",
        "File2_NumChunks",
        "Num_Deltas",
        "File1_Chunks_Same_As_Previous",
        "File2_Chunks_Same_As_Previous"
    ])

    # Run 20 times
    for run in range(1, 21):
        print(f"\n=== Run {run} ===")

        # Clean old output
        shutil.rmtree(cdc_dir_1, ignore_errors=True)
        shutil.rmtree(cdc_dir_2, ignore_errors=True)
        os.makedirs(cdc_dir_1, exist_ok=True)
        os.makedirs(cdc_dir_2, exist_ok=True)
        os.makedirs(delta_dir, exist_ok=True)
        # Chunk both files
        rabin_karp_cdc_stream_with_bounds(
            file1_path, cdc_dir_1,
            mask_bits=10, min_chunk=512, max_chunk=4096
        )
        rabin_karp_cdc_stream_with_bounds(
            file2_path, cdc_dir_2,
            mask_bits=10, min_chunk=512, max_chunk=4096
        )

        # Collect chunk hashes
        hashes1 = extract_hashes(cdc_dir_1)
        hashes2 = extract_hashes(cdc_dir_2)

        delta = Delta(cdc_dir_1, cdc_dir_2, delta_dir)

        # Generate deltas
        delta.generate_chunk_deltas()

        deltas = extract_hashes(delta_dir)

        # Check matches with previous run
        match1 = (hashes1 == previous_hashes_1) if previous_hashes_1 is not None else "N/A"
        match2 = (hashes2 == previous_hashes_2) if previous_hashes_2 is not None else "N/A"

        # Print to console
        print(f"File 1: {len(hashes1)} chunks")
        print(f"File 2: {len(hashes2)} chunks")
        print(f"Deltas created: {len(deltas)}")
        if match1 != "N/A":
            print(f"Chunks in file 1 same as previous run? {'YES' if match1 else 'NO'}")
            print(f"Chunks in file 2 same as previous run? {'YES' if match2 else 'NO'}")

        # Save to CSV
        writer.writerow([
            run,
            len(hashes1),
            len(hashes2),
            len(deltas),
            match1 if match1 == "N/A" else ("YES" if match1 else "NO"),
            match2 if match2 == "N/A" else ("YES" if match2 else "NO"),
        ])

        # Update previous hashes
        previous_hashes_1 = hashes1
        previous_hashes_2 = hashes2

print("\nExperiment completed! Results saved to", csv_file)