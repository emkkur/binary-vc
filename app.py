from modules.cdc import rabin_karp_cdc_stream_with_bounds
from modules.delta import Delta
import os

def process_files(original_file, modified_file):
    """Run full CDC + Delta Compression pipeline for two binary files."""
    working_dir = os.getcwd() + "/output"
    cdc_dir_1 = os.path.join(working_dir, "version1_chunks")
    cdc_dir_2 = os.path.join(working_dir, "version2_chunks")
    delta_dir = os.path.join(working_dir, "deltas")

    print("Running CDC on original file...")
    rabin_karp_cdc_stream_with_bounds(original_file, cdc_dir_1)

    print("Running CDC on modified file...")
    rabin_karp_cdc_stream_with_bounds(modified_file, cdc_dir_2)

    delta = Delta(cdc_dir_1, cdc_dir_2, delta_dir)

    print("Generating deltas...")
    delta.generate_chunk_deltas()

    print("Done. Chunks and deltas are stored in:", working_dir)

if __name__ == "__main__":
    modified = "/Users/emilkurian/Desktop/development/binary-vc/samples/modified.bmp"
    original = "/Users/emilkurian/Desktop/development/binary-vc/samples/original.bmp"
    process_files(original, modified)