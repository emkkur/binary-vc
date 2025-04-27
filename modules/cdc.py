import hashlib
import os
import json

def hash_concat_chunks(chunks_dir):
    h = hashlib.sha256()
    with open(os.path.join(chunks_dir, "manifest.json")) as mf:
        manifest = json.load(mf)
    for name in manifest:
        with open(os.path.join(chunks_dir, name), "rb") as f:
            h.update(f.read())
    return h.hexdigest()


def rabin_karp_cdc_stream_with_bounds(file_path, output_dir, window_size=64, mask_bits=15, min_chunk=8192, max_chunk=65536):
    mask = (1 << mask_bits) - 1
    base = 257
    mod = 1 << 64

    os.makedirs(output_dir, exist_ok=True)
    chunk_files = []
    seq = 0

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
                chunk_name = f"chunk_{seq:06d}_{chunk_hash}.bin"
                chunk_path = os.path.join(output_dir, chunk_name)
                with open(chunk_path, "wb") as chunk_file:
                    chunk_file.write(chunk)
                chunk_files.append(chunk_name)
                seq += 1
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
            chunk_name = f"chunk_{seq:06d}_{chunk_hash}.bin"
            chunk_path = os.path.join(output_dir, chunk_name)
            with open(chunk_path, "wb") as chunk_file:
                chunk_file.write(chunk)
            chunk_files.append(chunk_name)
            seq += 1

        manifest_path = os.path.join(output_dir, "manifest.json")
        with open(manifest_path, "w") as mf:
            json.dump(chunk_files, mf, indent=2)
    orig_hash = hashlib.sha256(open(file_path,"rb").read()).hexdigest()
    chunks_hash = hash_concat_chunks(output_dir)
    print(f"Checking files at {file_path} and output at {output_dir}")
    if (orig_hash != chunks_hash):
        raise RuntimeError(f"Chunking failed, hash does not match {chunk_hash}, {orig_hash}")
