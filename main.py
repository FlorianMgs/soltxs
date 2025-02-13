import json
import soltxs
from pprint import pprint

if __name__ == "__main__":
    with open("mint.json", "r") as f:
        tx = json.load(f)

    normalized = soltxs.normalize(tx)
    # pprint(normalized)
    parsed = soltxs.parse(normalized)
    pprint(parsed)
    # processed = soltxs.process(tx)
    # pprint(processed)
