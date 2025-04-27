import os
import json
import gzip
import struct
class Delta:
    def __init__(self, base_dir, modified_dir, output_dir):
        self.base_dir = base_dir
        self.modified_dir = modified_dir
        self.output_dir = output_dir

    def generate_chunk_deltas(self):
        """Generate delta files for each chunk in the modified stream, in manifest order."""
        os.makedirs(self.output_dir, exist_ok=True)

        with open(os.path.join(self.base_dir, "manifest.json")) as bf:
            base_manifest = json.load(bf)
        with open(os.path.join(self.modified_dir, "manifest.json")) as mf:
            mod_manifest = json.load(mf)

        for idx, mod_name in enumerate(mod_manifest):
            mod_path = os.path.join(self.modified_dir, mod_name)
            if idx < len(base_manifest):
                base_name = base_manifest[idx]
                base_path = os.path.join(self.base_dir, base_name)
                with open(base_path, "rb") as bf:
                    base_bytes = bf.read()
            else:
                base_bytes = b""

            # Always load the modified chunk
            with open(mod_path, "rb") as mf:
                mod_bytes = mf.read()

            delta = []
            for i in range(max(len(base_bytes), len(mod_bytes))):
                b = base_bytes[i] if i < len(base_bytes) else None
                m = mod_bytes[i] if i < len(mod_bytes) else None
                if b != m:
                    delta.append((i, m))

            # Determine per-byte diff overhead
            per_edit_overhead = 4 + 1
            raw_size = len(mod_bytes)
            delta_overhead = len(delta) * per_edit_overhead

            # Build run-length encoding for contiguous edits
            runs = []
            if delta:
                run_start, first_val = delta[0]
                run_bytes = bytearray([first_val if first_val is not None else 0])
                prev_idx = run_start
                for idx_i, val in delta[1:]:
                    if idx_i == prev_idx + 1:
                        run_bytes.append(val if val is not None else 0)
                    else:
                        runs.append((run_start, run_bytes))
                        run_start = idx_i
                        run_bytes = bytearray([val if val is not None else 0])
                    prev_idx = idx_i
                runs.append((run_start, run_bytes))

            # Choose representation: full chunk, run-length, or per-byte
            if delta_overhead > raw_size:
                mode = 'full'
            else:
                # Compute run-length overhead
                run_overhead = sum(8 + len(b) for (_, b) in runs)
                mode = 'run' if run_overhead < delta_overhead else 'delta'

            # Write compressed delta file
            delta_name = f"{mode}_{idx:06d}.bin.gz"
            delta_path = os.path.join(self.output_dir, delta_name)
            with gzip.open(delta_path, "wb") as out:
                if mode == 'full':
                    out.write(mod_bytes)
                elif mode == 'run':
                    # write number of runs
                    out.write(struct.pack('<I', len(runs)))
                    for start, buf in runs:
                        out.write(struct.pack('<I', start))
                        out.write(struct.pack('<I', len(buf)))
                        out.write(buf)
                else:
                    # per-byte diffs
                    for index, val in delta:
                        out.write(struct.pack('<I', index))
                        out.write(bytes([val if val is not None else 0]))

    def reconstruct_chunk(self, base_chunk_path, delta_chunk_path, output_path):
        """Reconstruct a modified chunk from a base chunk and delta file."""
        # 1) Load base chunk if available, else start empty
        try:
            with open(base_chunk_path, "rb") as bf:
                data = bytearray(bf.read())
        except FileNotFoundError:
            data = bytearray()

        # 2) Apply delta
        if delta_chunk_path.endswith('.gz'):
            # Compressed delta
            content = gzip.open(delta_chunk_path, "rb").read()
            basename = os.path.basename(delta_chunk_path)
            if basename.startswith('full_'):
                # Full chunk stored
                data = bytearray(content)
            elif basename.startswith('run_'):
                # Run-length encoded
                offset = 0
                num_runs = struct.unpack_from('<I', content, offset)[0]
                offset += 4
                for _ in range(num_runs):
                    start = struct.unpack_from('<I', content, offset)[0]
                    offset += 4
                    length = struct.unpack_from('<I', content, offset)[0]
                    offset += 4
                    run_data = content[offset:offset+length]
                    offset += length
                    if start < len(data):
                        data[start:start+length] = run_data
                    else:
                        data.extend(b'\x00' * (start - len(data)))
                        data.extend(run_data)
            else:
                # Per-byte diffs
                mv = memoryview(content)
                offset = 0
                while offset + 5 <= len(mv):
                    idx = struct.unpack_from('<I', mv, offset)[0]
                    offset += 4
                    val = mv[offset]
                    offset += 1
                    if idx < len(data):
                        data[idx] = val
                    else:
                        data.extend(b'\x00' * (idx - len(data)))
                        data.append(val)
        else:
            # Uncompressed per-byte delta (.bin)
            with open(delta_chunk_path, "rb") as df:
                while True:
                    idx_bytes = df.read(4)
                    if not idx_bytes:
                        break
                    val_byte = df.read(1)
                    if not val_byte:
                        break
                    idx = int.from_bytes(idx_bytes, 'little')
                    val = val_byte[0]
                    if idx < len(data):
                        data[idx] = val
                    else:
                        data.extend(b'\x00' * (idx - len(data)))
                        data.append(val)

        # 3) Write reconstructed chunk
        with open(output_path, "wb") as out:
            out.write(data)