from typing import Any, Dict, List
from itertools import chain

from soltxs.normalizer.models import Transaction
from soltxs.parser import addons, models, parsers

# Map program IDs to their corresponding parser classes.
id_to_handler: Dict[str, models.Program] = {
    parsers.systemProgram.SystemProgramParser.program_id: parsers.systemProgram.SystemProgramParser,
    parsers.computeBudget.ComputeBudgetParser.program_id: parsers.computeBudget.ComputeBudgetParser,
    parsers.tokenProgram.TokenProgramParser.program_id: parsers.tokenProgram.TokenProgramParser,
    parsers.raydiumAMM.RaydiumAMMParser.program_id: parsers.raydiumAMM.RaydiumAMMParser,
    parsers.pumpfun.PumpFunParser.program_id: parsers.pumpfun.PumpFunParser,
    parsers.mortem.MortemParser.program_id: parsers.mortem.MortemParser,
}

# List of addon enrichers for additional data.
addon_enrichers: List[models.Addon] = [
    addons.compute_units.ComputeUnitsAddon,
    addons.instruction_count.InstructionCountAddon,
    addons.loaded_addresses.LoadedAddressesAddon,
    addons.platform_identifier.PlatformIdentifierAddon,
    addons.token_transfer.TokenTransferSummaryAddon,
]


def deduplicate_instructions(instructions: List[Any]) -> List[Any]:
    """
    Over-engineered method to quickly deduplicate parsed instructions for those with class names "Buy" or "Sell".
    
    This version:
      - Uses itertools.chain.from_iterable to flatten the incoming instructions on the fly.
      - Discards None values.
      - For Buy/Sell instructions, deduplicates using a key based on:
          (signature, instruction_name, who, from_token, to_token,
           from_token_decimals, to_token_decimals, from_token_amount, to_token_amount)
      - Ignores the program_id key altogether for efficiency.
    
    Returns:
        A deduplicated list of instructions, preserving the order.
    """
    # Create a flattened iterator for all instructions (skipping any None).
    flat_iter = chain.from_iterable(
        (item if isinstance(item, list) else [item])
        for item in instructions if item is not None
    )
    
    seen = set()
    deduped = []
    # Do all processing in a single loop
    for ins in flat_iter:
        cls_name = ins.__class__.__name__
        if cls_name in ("Buy", "Sell", "Swap"):
            key = (
                ins.signature,
                ins.instruction_name,
                ins.who,
                ins.from_token,
                ins.to_token,
                ins.from_token_decimals,
                ins.to_token_decimals,
                ins.from_token_amount,
                ins.to_token_amount,
            )
            if key in seen:
                continue
            seen.add(key)
        deduped.append(ins)
    
    return deduped


def parse(tx: Transaction) -> Dict[str, Any]:
    """
    Parses a normalized transaction into its component instructions and addon data.

    Args:
        tx: A normalized Transaction object.

    Returns:
        A dictionary containing:
          - "signatures": List of transaction signatures.
          - "instructions": List of parsed (and deduplicated) instruction objects.
          - "addons": Dictionary of addon enrichment data.
    """
    parsed_instructions = []

    for idx, instruction in enumerate(tx.message.instructions):
        # Determine the program id for the instruction.
        program_id = tx.message.accountKeys[instruction.programIdIndex]
        # Select the appropriate parser; default to UnknownParser if not found.
        router = id_to_handler.get(program_id, parsers.unknown.UnknownParser(program_id))
        action = router.route(tx, idx)
        parsed_instructions.append(action)

    # Deduplicate instructions across the transaction.
    dedup_instructions = deduplicate_instructions(parsed_instructions)

    addons_result: Dict[str, Any] = {}
    for addon in addon_enrichers:
        result = addon.enrich(tx)
        if result is not None:
            addons_result[addon.addon_name] = result

    return {
        "signatures": tx.signatures,
        "instructions": dedup_instructions,
        "addons": addons_result,
    }
