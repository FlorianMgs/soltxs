import json
import soltxs
from pprint import pprint
import time

if __name__ == "__main__":
    with open("example_txs/unknown/raydium2.json", "r") as f:
        tx = json.load(f)

    print("## NORMALIZED")
    normalized = soltxs.normalize(tx)
    pprint(normalized)
    print("-" * 100)
    print("## PARSED")
    start = time.time()
    parsed = soltxs.parse(normalized)
    end = time.time()
    print(f"Time taken: {end - start} seconds")
    pprint(parsed)
    print("-" * 100)
    resolved = soltxs.resolve(parsed)
    pprint(resolved)
    # print("-" * 100)
    # processed = soltxs.process(tx)
    # pprint(processed)
