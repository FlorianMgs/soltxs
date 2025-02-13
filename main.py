import json
import soltxs
from pprint import pprint

if __name__ == "__main__":
    with open("bot_sell.json", "r") as f:
        tx = json.load(f)

    normalized = soltxs.normalize(tx)
    pprint(normalized)
    print("-" * 100)
    parsed = soltxs.parse(normalized)
    pprint(parsed)
    print("-" * 100)
    # resolved = soltxs.resolve(parsed)
    # pprint(resolved)
    # print("-" * 100)
    # processed = soltxs.process(tx)
    # pprint(processed)
