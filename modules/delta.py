import os
import json
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

            delta_name = f"delta_{idx:06d}.bin"
            delta_path = os.path.join(self.output_dir, delta_name)
            with open(delta_path, "wb") as out:
                for index, value in delta:
                    out.write(index.to_bytes(4, "little"))
                    out.write(bytes([value if value is not None else 0]))

    def reconstruct_chunk(self, base_chunk_path, delta_chunk_path, output_path):
        """Reconstruct a modified chunk from a base chunk and delta file."""
        # Load base chunk if available, else start from empty for new chunks
        try:
            with open(base_chunk_path, "rb") as f:
                data = bytearray(f.read())
        except FileNotFoundError:
            data = bytearray()

        with open(delta_chunk_path, "rb") as f:
            while True:
                index_bytes = f.read(4)
                if not index_bytes:
                    break
                value_byte = f.read(1)
                if not value_byte:
                    break
                index = int.from_bytes(index_bytes, 'little')
                value = value_byte[0]
                if index < len(data):
                    data[index] = value
                else:
                    data.extend([0] * (index - len(data)))
                    data.append(value)
    
        with open(output_path, "wb") as f:
            f.write(data)