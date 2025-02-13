import json
import soltxs
from pprint import pprint

if __name__ == "__main__":
    with open("pump.json", "r") as f:
        tx = json.load(f)

    normalized = soltxs.normalize(tx)
    # pprint(normalized)
    parsed = soltxs.parse(normalized)
    pprint(parsed.get("instructions"))
    print("-" * 100)
    processed = soltxs.process(tx)
    pprint(processed)
