import json
import soltxs
from pprint import pprint

if __name__ == "__main__":
    with open("tx.json", "r") as f:
        tx = json.load(f)

    processed = soltxs.process(tx)
    pprint(processed)
