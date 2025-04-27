import os
import shutil
import csv
import time
from modules.cdc import rabin_karp_cdc_stream_with_bounds
from modules.delta import Delta

# Paths to your two images
file1_path = "/Users/emilkurian/Desktop/development/binary-vc/samples/original.bmp"
file2_path = "/Users/emilkurian/Desktop/development/binary-vc/samples/modified.bmp"

# Temporary folders
cdc_dir_1 = "output/temp_chunks_1"
cdc_dir_2 = "output/temp_chunks_2"
delta_dir = "output/deltas"
# CSV output file
csv_file = "chunking_experiment_results.csv"

# Function to extract hashes from filenames
def extract_hashes(directory):
    """Extract base filenames (without extensions) for .bin and .bin.gz files."""
    hashes = []
    for f in os.listdir(directory):
        if f.endswith('.bin') or f.endswith('.bin.gz'):
            hashes.append(f.split('.')[0])
    return sorted(hashes)

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
        "TotalDeltaSize",
        "DeltaCompressionRatio",
        "File1_AvgChunkSize",
        "File2_AvgChunkSize",
        "RuntimeSeconds",
        "File1_Chunks_Same_As_Previous",
        "File2_Chunks_Same_As_Previous"
    ])

    # Run 20 times
    for run in range(1, 21):
        print(f"\n=== Run {run} ===")
        start_time = time.time()

        # Clean old output
        shutil.rmtree(cdc_dir_1, ignore_errors=True)
        shutil.rmtree(cdc_dir_2, ignore_errors=True)
        os.makedirs(cdc_dir_1, exist_ok=True)
        os.makedirs(cdc_dir_2, exist_ok=True)
        os.makedirs(delta_dir, exist_ok=True)
        # Chunk both files
        rabin_karp_cdc_stream_with_bounds(
            file1_path, cdc_dir_1,
        )
        rabin_karp_cdc_stream_with_bounds(
            file2_path, cdc_dir_2,
        )

        # Collect chunk hashes
        hashes1 = extract_hashes(cdc_dir_1)
        hashes2 = extract_hashes(cdc_dir_2)

        delta = Delta(cdc_dir_1, cdc_dir_2, delta_dir)

        # Generate deltas
        delta.generate_chunk_deltas()

        deltas = extract_hashes(delta_dir)

        # Compute average chunk sizes
        sizes1 = [os.path.getsize(os.path.join(cdc_dir_1, f)) for f in os.listdir(cdc_dir_1) if f.endswith('.bin')]
        sizes2 = [os.path.getsize(os.path.join(cdc_dir_2, f)) for f in os.listdir(cdc_dir_2) if f.endswith('.bin')]
        avg1 = sum(sizes1) / len(sizes1) if sizes1 else 0
        avg2 = sum(sizes2) / len(sizes2) if sizes2 else 0

        # Check matches with previous run
        match1 = (hashes1 == previous_hashes_1) if previous_hashes_1 is not None else "N/A"
        match2 = (hashes2 == previous_hashes_2) if previous_hashes_2 is not None else "N/A"

        runtime = time.time() - start_time

        # Compute total delta size and compression ratio (including .bin.gz)
        delta_sizes = [
            os.path.getsize(os.path.join(delta_dir, f))
            for f in os.listdir(delta_dir)
            if f.endswith('.bin') or f.endswith('.bin.gz')
        ]
        total_delta_size = sum(delta_sizes)
        modified_size = os.path.getsize(file2_path)
        compression_ratio = total_delta_size / modified_size if modified_size else 0

        # Print to console
        print(f"File 1: {len(hashes1)} chunks")
        print(f"File 2: {len(hashes2)} chunks")
        print(f"Deltas created: {len(deltas)}")
        print(f"Avg chunk size file 1: {avg1:.2f} bytes")
        print(f"Avg chunk size file 2: {avg2:.2f} bytes")
        print(f"Run time: {runtime:.3f} sec")
        if match1 != "N/A":
            print(f"Chunks in file 1 same as previous run? {'YES' if match1 else 'NO'}")
            print(f"Chunks in file 2 same as previous run? {'YES' if match2 else 'NO'}")
        print(f"Modified file size {modified_size}")
        print(f"Total delta size: {total_delta_size} bytes")
        print(f"Delta compression ratio: {compression_ratio:.3f}")

        # Save to CSV
        writer.writerow([
            run,
            len(hashes1),
            len(hashes2),
            len(deltas),
            total_delta_size,
            compression_ratio,
            avg1,
            avg2,
            runtime,
            match1 if match1 == "N/A" else ("YES" if match1 else "NO"),
            match2 if match2 == "N/A" else ("YES" if match2 else "NO"),
        ])

        # Update previous hashes
        previous_hashes_1 = hashes1
        previous_hashes_2 = hashes2

print("\nExperiment completed! Results saved to", csv_file)