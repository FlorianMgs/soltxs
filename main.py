import json
import soltxs
from pprint import pprint

if __name__ == "__main__":
    with open("pump.json", "r") as f:
        tx = json.load(f)

    parsed = soltxs.normalize(tx)
    pprint(parsed)
    processed = soltxs.process(tx)
    pprint(processed)
