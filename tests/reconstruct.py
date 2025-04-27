import os
import sys
import shutil
import json
import hashlib
from modules.cdc import rabin_karp_cdc_stream_with_bounds
from modules.delta import Delta

def reconstruct_modified_file(original_file, deltas_dir, output_file, temp_dir="reconstruct_temp"):
    """
    Reconstruct the modified file from the original file and delta chunks only.
    - original_file: path to the original binary
    - deltas_dir:   path to folder containing delta_<index>.bin files
    - output_file:  path where reconstructed file will be written
    """
    # 1) Prepare temporary directories
    base_chunks_dir    = os.path.join(temp_dir, "base_chunks")
    reconstructed_dir  = os.path.join(temp_dir, "reconstructed_chunks")
    shutil.rmtree(temp_dir, ignore_errors=True)
    os.makedirs(base_chunks_dir, exist_ok=True)
    os.makedirs(reconstructed_dir, exist_ok=True)

    # 2) Chunk the original file (CDC) → emits chunk_*.bin + manifest.json
    rabin_karp_cdc_stream_with_bounds(original_file, base_chunks_dir)

    # 3) Load the base manifest to know chunk order
    manifest_path = os.path.join(base_chunks_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Missing manifest.json in {base_chunks_dir}")
    with open(manifest_path, "r") as mf:
        base_manifest = json.load(mf)

    # 4) Gather and sort delta files by sequence index
    delta_files = sorted(
        f for f in os.listdir(deltas_dir)
        if f.startswith("delta_") and f.endswith(".bin")
    )

    # 5) Reconstruct chunks
    delta_helper = Delta(None, None, None)
    for delta_file in delta_files:
        idx = int(delta_file.split("_")[1].split(".")[0])
        delta_path = os.path.join(deltas_dir, delta_file)
        out_chunk_name = f"chunk_{idx:06d}.bin"
        out_chunk_path = os.path.join(reconstructed_dir, out_chunk_name)

        if idx < len(base_manifest):
            # Apply delta to the existing base chunk
            base_chunk_name = base_manifest[idx]
            base_chunk_path = os.path.join(base_chunks_dir, base_chunk_name)
            delta_helper.reconstruct_chunk(base_chunk_path, delta_path, out_chunk_path)
        else:
            # New chunk beyond original: reconstruct from empty
            data = bytearray()
            with open(delta_path, "rb") as df:
                while True:
                    idx_bytes = df.read(4)
                    if not idx_bytes:
                        break
                    value_byte = df.read(1)
                    if not value_byte:
                        break
                    pos = int.from_bytes(idx_bytes, "little")
                    val = value_byte[0]
                    if pos < len(data):
                        data[pos] = val
                    else:
                        data.extend([0] * (pos - len(data)))
                        data.append(val)
            with open(out_chunk_path, "wb") as of:
                of.write(data)

    # 6) Merge reconstructed chunks in delta order into the final file
    with open(output_file, "wb") as outf:
        for delta_file in delta_files:
            idx = int(delta_file.split("_")[1].split(".")[0])
            chunk_name = f"chunk_{idx:06d}.bin"
            chunk_path = os.path.join(reconstructed_dir, chunk_name)
            with open(chunk_path, "rb") as cf:
                outf.write(cf.read())

    # 7) Optional: verify against true modified file (if provided as 4th CLI arg)
    if len(sys.argv) >= 4:
        true_modified = sys.argv[3]
        def sha256(path):
            h = hashlib.sha256()
            with open(path, "rb") as f:
                for block in iter(lambda: f.read(8192), b""):
                    h.update(block)
            return h.hexdigest()
        print("Modified file hash:  ", sha256(true_modified))
        print("Recreated file hash: ", sha256(output_file))

    # 8) Clean up
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"\n✔️ Reconstructed file written to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} ORIGINAL_FILE DELTAS_DIR [TRUE_MODIFIED_FILE]")
        sys.exit(1)
    orig     = sys.argv[1]
    deltas   = sys.argv[2]
    out_file = sys.argv[3] if len(sys.argv) >= 4 else "recreated_file.bin"
    reconstruct_modified_file(orig, deltas, out_file)