from dataclasses import dataclass
from typing import Union

import base64
import qbase58 as base58

from soltxs.normalizer.models import Transaction
from soltxs.parser.models import ParsedInstruction, Program
from soltxs.parser.parsers.mortem import Buy, Sell, WSOL_MINT, SOL_DECIMALS, SwapData, PUMPFUN_PROGRAM_ID

BLACKLIST_PROGRAM_IDS = {
    "111111111111111111111111111111111",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
}

@dataclass(slots=True)
class Unknown(ParsedInstruction):
    """
    Parsed instruction representing an unknown instruction.

    Attributes:
        instruction_index: The index of the unknown instruction.
    """
    instruction_index: int

ParsedInstructions = Union[Unknown, Buy, Sell]


class UnknownParser(Program):
    """
    Parser for unknown instructions. This parser looks through inner instructions,
    top-level instructions, and log events for pump fun swap events. If a swap event is
    detected—and its originating program ID is not blacklisted, and it is not a duplicate—
    then a corresponding Buy or Sell parsed instruction is returned. Otherwise, a generic Unknown
    instruction is returned.
    """
    def __init__(self, program_id: str) -> None:
        self.program_id = program_id  # desired allowed program id (e.g. FAdo9NCw1ssek6Z6yeWzWjhLVsr8uiCwcWNUnKgzTnHe)
        self.program_name = "Unknown"
        self.desc = lambda d: True
        self.desc_map = {True: self.process_unknown}

    def process_unknown(
        self,
        tx: Transaction,
        instruction_index: int,
        decoded_data: bytes,
    ) -> ParsedInstructions:
        """
        Processes an unknown instruction. Scans inner instructions and top-level instructions for pump fun swap events,
        attaches their originating program id, and filters out events whose program id belongs to the blacklist.
        Then, duplicate events are removed based on a unique key (signature, program_id, instruction_name, who,
        from_token, to_token, decimals, and amounts). If an allowed unique swap event is found, returns a Buy or Sell instruction.
        Otherwise, falls back to returning a generic Unknown instruction.
        """
        swap_events_inner = self._parse_swap_from_inner(tx)
        swap_events_instructions = self._parse_swap_from_instructions(tx)
        # Each item is a tuple: (swap_event, origin_program_id)
        swap_events = swap_events_inner + swap_events_instructions

        # Filter out events with blacklisted program IDs.
        allowed_events = [
            (swap, origin) for swap, origin in swap_events if origin not in BLACKLIST_PROGRAM_IDS
        ]

        # Build a unique set based on all key parsed fields.
        unique_events = []
        seen = set()
        signature = tx.signatures[0]
        for swap, origin in allowed_events:
            is_buy = bool(self._get_field(swap, "is_buy"))
            user = str(self._get_field(swap, "user"))
            mint = str(self._get_field(swap, "mint"))
            sol_amount = int(self._get_field(swap, "sol_amount"))
            token_amount = int(self._get_field(swap, "token_amount"))
            if is_buy:
                # For buy: from_token = WSOL_MINT and to_token = mint.
                from_token = WSOL_MINT
                from_decimals = SOL_DECIMALS
                to_token = mint
                to_decimals = self._get_token_decimals(tx, mint)
                instruction_name = "Buy"
                # In Buy, the amounts are: from_token_amount = sol_amount, to_token_amount = token_amount.
                key = (
                    signature,
                    origin,
                    instruction_name,
                    user,
                    from_token,
                    from_decimals,
                    to_token,
                    to_decimals,
                    sol_amount,
                    token_amount,
                )
            else:
                # For sell: from_token = mint and to_token = WSOL_MINT.
                from_token = mint
                from_decimals = self._get_token_decimals(tx, mint)
                to_token = WSOL_MINT
                to_decimals = SOL_DECIMALS
                instruction_name = "Sell"
                # In Sell, the amounts are: from_token_amount = token_amount, to_token_amount = sol_amount.
                key = (
                    signature,
                    origin,
                    instruction_name,
                    user,
                    from_token,
                    from_decimals,
                    to_token,
                    to_decimals,
                    token_amount,
                    sol_amount,
                )
            if key not in seen:
                seen.add(key)
                unique_events.append((swap, origin))

        if unique_events:
            # Prefer a Buy event if any unique allowed swap event is flagged as buy.
            for swap, origin in unique_events:
                if self._get_field(swap, "is_buy"):
                    return self._build_buy(tx, swap, origin)
            # Otherwise, return a Sell event.
            for swap, origin in unique_events:
                if not self._get_field(swap, "is_buy"):
                    return self._build_sell(tx, swap, origin)

        if tx.meta and hasattr(tx.meta, "logMessages"):
            for log in tx.meta.logMessages:
                if "Instruction: Buy" in log or "Instruction: Sell" in log:
                    break

        return Unknown(
            program_id=self.program_id,
            program_name=self.program_name,
            instruction_name="Unknown",
            instruction_index=instruction_index,
        )

    def _parse_swap_from_inner(self, tx: Transaction) -> list[tuple[SwapData, str]]:
        """
        Parses swap events from the transaction's inner instructions. Only inner instructions
        with a pump fun program ID are considered.
        Returns a list of tuples containing the SwapData and the originating program id.
        """
        events: list[tuple[SwapData, str]] = []
        if not (tx.meta and hasattr(tx.meta, "innerInstructions")):
            return events

        for group in tx.meta.innerInstructions:
            instructions = group.get("instructions", [])
            for in_instr in instructions:
                sub_prog_id = tx.all_accounts[in_instr["programIdIndex"]]
                if sub_prog_id != PUMPFUN_PROGRAM_ID:
                    continue
                try:
                    raw_data = base58.decode(in_instr.get("data", ""))
                except Exception:
                    try:
                        raw_data = base64.b64decode(in_instr.get("data", ""))
                    except Exception:
                        continue
                if len(raw_data) < 48:
                    continue
                swap_raw = raw_data[16:]
                try:
                    swap_obj = SwapData.decode(swap_raw)
                except Exception:
                    continue
                events.append((swap_obj, sub_prog_id))
        return events

    def _parse_swap_from_instructions(self, tx: Transaction) -> list[tuple[SwapData, str]]:
        """
        Parses swap events from the transaction's top-level instructions. Only instructions
        with a pump fun program ID are considered.
        Returns a list of tuples containing the SwapData and its originating program id.
        """
        events: list[tuple[SwapData, str]] = []
        if not (tx.message and hasattr(tx.message, "instructions")):
            return events

        for instr in tx.message.instructions:
            sub_prog_id = tx.message.accountKeys[instr.programIdIndex]
            if sub_prog_id != PUMPFUN_PROGRAM_ID:
                continue
            try:
                raw_data = base58.decode(instr.data)
            except Exception:
                try:
                    raw_data = base64.b64decode(instr.data)
                except Exception:
                    continue
            if len(raw_data) < 48:
                continue
            swap_raw = raw_data[16:]
            try:
                swap_obj = SwapData.decode(swap_raw)
            except Exception:
                continue
            events.append((swap_obj, sub_prog_id))
        return events

    def _build_buy(self, tx: Transaction, swap: SwapData, origin_program_id: str) -> Buy:
        """
        Constructs a Buy parsed instruction based on a pump fun swap event.
        The returned instruction's program_id is set to the event's originating program id.
        """
        who: str = str(self._get_field(swap, "user"))
        from_token: str = WSOL_MINT
        to_token: str = str(self._get_field(swap, "mint"))
        from_amount: int = int(self._get_field(swap, "sol_amount"))
        to_amount: int = int(self._get_field(swap, "token_amount"))
        from_decimals: int = SOL_DECIMALS
        to_decimals: int = self._get_token_decimals(tx, to_token)
        return Buy(
            signature=tx.signatures[0],
            program_id=origin_program_id,
            program_name="PumpFun-Buy",
            instruction_name="Buy",
            who=who,
            from_token=from_token,
            from_token_decimals=from_decimals,
            to_token=to_token,
            to_token_decimals=to_decimals,
            from_token_amount=from_amount,
            to_token_amount=to_amount,
        )

    def _build_sell(self, tx: Transaction, swap: SwapData, origin_program_id: str) -> Sell:
        """
        Constructs a Sell parsed instruction based on a pump fun swap event.
        The returned instruction's program_id is set to the event's originating program id.
        """
        who: str = str(self._get_field(swap, "user"))
        from_token: str = str(self._get_field(swap, "mint"))
        to_token: str = WSOL_MINT
        from_amount: int = int(self._get_field(swap, "token_amount"))
        to_amount: int = int(self._get_field(swap, "sol_amount"))
        from_decimals: int = self._get_token_decimals(tx, from_token)
        to_decimals: int = SOL_DECIMALS
        return Sell(
            signature=tx.signatures[0],
            program_id=origin_program_id,
            program_name="PumpFun-Sell",
            instruction_name="Sell",
            who=who,
            from_token=from_token,
            from_token_decimals=from_decimals,
            to_token=to_token,
            to_token_decimals=to_decimals,
            from_token_amount=from_amount,
            to_token_amount=to_amount,
        )

    def _get_token_decimals(self, tx: Transaction, mint: str) -> int:
        """
        Retrieves the decimals for a given token mint from the transaction's pre and post token balances.
        """
        if mint == WSOL_MINT:
            return SOL_DECIMALS
        if tx.meta:
            balances = (tx.meta.preTokenBalances or []) + (tx.meta.postTokenBalances or [])
            for tb in balances:
                if tb.mint == mint:
                    return tb.uiTokenAmount.decimals
        raise ValueError(f"Could not find decimals for mint {mint}")

    def _get_field(self, data, field: str):
        """
        Helper method to retrieve a field's value from the swap data.
        Works with both dicts and objects.
        """
        if isinstance(data, dict):
            return data.get(field)
        return getattr(data, field)
