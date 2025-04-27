import hashlib
import os
import json

def rabin_karp_cdc_stream_with_bounds(file_path, output_dir, window_size=64, mask_bits=9, min_chunk=512, max_chunk=4096):
    mask = (1 << mask_bits) - 1
    base = 257
    mod = 1 << 64

    os.makedirs(output_dir, exist_ok=True)
    chunk_files = []

    with open(file_path, "rb") as f:
        buffer = f.read(window_size)
        if not buffer:
            return

        h = 0
        power = 1
        for k in range(len(buffer)):
            h = ((h * base) + buffer[k]) & (mod - 1)
            if k < window_size - 1:
                power = (power * base) & (mod - 1)

        chunk = bytearray(buffer)
        while True:
            byte = f.read(1)
            if not byte:
                break

            chunk.append(byte[0])

            if len(chunk) >= min_chunk and ((h & mask) == 0 or len(chunk) >= max_chunk):
                chunk_hash = hashlib.sha256(chunk).hexdigest()[:16]
                chunk_name = f"chunk_{chunk_hash}.bin"
                chunk_path = os.path.join(output_dir, chunk_name)
                with open(chunk_path, "wb") as chunk_file:
                    chunk_file.write(chunk)
                print(f"Debug {chunk_hash} point 1")
                chunk_files.append(chunk_name)
                buffer = f.read(window_size)
                if not buffer:
                    return

                chunk = bytearray(buffer)
                h = 0
                power = 1
                for k in range(len(buffer)):
                    h = ((h * base) + buffer[k]) & (mod - 1)
                    if k < window_size - 1:
                        power = (power * base) & (mod - 1)
                continue

            old_byte = chunk[-window_size]
            h = (h - (old_byte * power) & (mod - 1)) & (mod - 1)
            h = ((h * base) + byte[0]) & (mod - 1)

        if chunk:
            chunk_hash = hashlib.sha256(chunk).hexdigest()[:16]
            chunk_name = f"chunk_{chunk_hash}.bin"
            chunk_path = os.path.join(output_dir, chunk_name)
            with open(chunk_path, "wb") as chunk_file:
                chunk_file.write(chunk)
            print(f"Debug {chunk_hash} point 2")
            chunk_files.append(chunk_name)

        manifest_path = os.path.join(output_dir, "manifest.json")
        with open(manifest_path, "w") as mf:
            json.dump(chunk_files, mf, indent=2)
