import base64
import qbase58 as base58


def make_readable(data: str) -> str:
    """
    Convert a base64-encoded program ID to a base58-encoded Solana address string.

    Args:
        program_id_b64: A string containing the base64-encoded program ID.

    Returns:
        A string with the base58-encoded Solana address.
    """
    # Decode the base64 string to get raw bytes.
    raw_bytes: bytes = base64.b64decode(data)
    # Encode the raw bytes into a base58 representation.
    # qbase58.b58encode returns a bytes object so we decode it to str if necessary.
    b58_encoded = base58.encode(raw_bytes)
    readable = b58_encoded.decode("utf-8") if isinstance(b58_encoded, bytes) else b58_encoded
    return readable
