#!/usr/bin/env python3
import os
import shutil
import json
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
    for fn in sorted(os.listdir(DELTAS_DIR)):
        if not fn.startswith("delta_"):
            continue

        # Determine sequence index from delta filename
        idx = int(fn.split("_")[1].split(".")[0])
        delta_path = os.path.join(DELTAS_DIR, fn)

        if idx < len(manifest):
            # Existing chunk: apply delta to the corresponding base chunk
            base_name = manifest[idx]
            base_path = os.path.join(base_chunks_dir, base_name)
            out_name  = base_name
            out_path  = os.path.join(out_chunks_dir, out_name)
            delta.reconstruct_chunk(base_path, delta_path, out_path)
        else:
            # New chunk beyond original: reconstruct from empty
            out_name = f"chunk_{idx:06d}.bin"
            out_path = os.path.join(out_chunks_dir, out_name)
            data = bytearray()
            with open(delta_path, "rb") as df:
                while True:
                    index_bytes = df.read(4)
                    if not index_bytes:
                        break
                    value_byte = df.read(1)
                    if not value_byte:
                        break
                    pos = int.from_bytes(index_bytes, "little")
                    val = value_byte[0]
                    if pos < len(data):
                        data[pos] = val
                    else:
                        data.extend([0] * (pos - len(data)))
                        data.append(val)
            with open(out_path, "wb") as of:
                of.write(data)

    with open(OUTPUT_FILE, "wb") as outf:
        # Write chunks in the exact delta sequence order
        delta_files = sorted(
            f for f in os.listdir(DELTAS_DIR)
            if f.startswith("delta_") and f.endswith(".bin")
        )
        for fn in delta_files:
            idx = int(fn.split("_")[1].split(".")[0])
            # Determine chunk filename from manifest or default for new chunks
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