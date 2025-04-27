#!/usr/bin/env python3
import os
import shutil
import json
import gzip
from modules.cdc import rabin_karp_cdc_stream_with_bounds
from modules.delta import Delta


ORIGINAL_FILE = "/Users/emilkurian/Desktop/development/binary-vc/samples/original.bmp"
DELTAS_DIR    = "/Users/emilkurian/Desktop/development/binary-vc/output/deltas"
OUTPUT_FILE   = "recreated.bmp"


def reconstruct():
    temp_dir          = "reconstruct_temp"
    base_chunks_dir   = os.path.join(temp_dir, "base_chunks")
    out_chunks_dir    = os.path.join(temp_dir, "reconstructed_chunks")

    shutil.rmtree(temp_dir, ignore_errors=True)
    os.makedirs(base_chunks_dir)
    os.makedirs(out_chunks_dir)

    rabin_karp_cdc_stream_with_bounds(
        ORIGINAL_FILE,
        base_chunks_dir
    )

    # Load base manifest to map delta indices to chunk filenames
    manifest_path = os.path.join(base_chunks_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Missing manifest.json in {base_chunks_dir}")
    with open(manifest_path, "r") as mf:
        manifest = json.load(mf)

    delta = Delta(base_chunks_dir, None, None)
    # Process all gzip-compressed delta files, sorted by their numeric index
    delta_files = [
        f for f in os.listdir(DELTAS_DIR)
        if (f.startswith("full_") or f.startswith("run_") or f.startswith("delta_"))
           and f.endswith(".bin.gz")
    ]
    delta_files.sort(key=lambda fn: int(fn.split("_")[1].split(".")[0]))
    for fn in delta_files:
        idx = int(fn.split("_")[1].split(".")[0])
        delta_path = os.path.join(DELTAS_DIR, fn)
        # Determine base chunk name (or default for new chunks)
        if idx < len(manifest):
            chunk_name = manifest[idx]
        else:
            chunk_name = f"chunk_{idx:06d}.bin"
        base_path = os.path.join(base_chunks_dir, chunk_name)
        out_path = os.path.join(out_chunks_dir, chunk_name)
        # Use Delta.reconstruct_chunk for both existing and new chunks
        delta.reconstruct_chunk(base_path, delta_path, out_path)

    with open(OUTPUT_FILE, "wb") as outf:
        for fn in delta_files:
            idx = int(fn.split("_")[1].split(".")[0])
            # Use the same chunk_name logic as above
            if idx < len(manifest):
                chunk_name = manifest[idx]
            else:
                chunk_name = f"chunk_{idx:06d}.bin"
            chunk_path = os.path.join(out_chunks_dir, chunk_name)
            with open(chunk_path, "rb") as cf:
                outf.write(cf.read())

    shutil.rmtree(temp_dir)
    print(f"\n✔ Reconstructed file written to: {OUTPUT_FILE}")

if __name__ == "__main__":
    reconstruct()