from dataclasses import dataclass
from typing import Union

import base64
import qbase58 as base58

from soltxs.normalizer.models import Transaction
from soltxs.parser.models import ParsedInstruction, Program
from soltxs.parser.parsers.mortem import Buy, Sell, WSOL_MINT, SOL_DECIMALS, SwapData, PUMPFUN_PROGRAM_ID
from soltxs.parser.parsers.raydiumAMM import Swap  # now include Swap in the union

RAYDIUM_AMM_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
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

ParsedInstructions = Union[Unknown, Buy, Sell, Swap]


class UnknownParser(Program):
    """
    Parser for unknown instructions. This parser looks through inner instructions,
    top-level instructions and log events for pump fun swap events. If a swap event is
    detected—and its originating program ID is not blacklisted, and it is not a duplicate—
    then a corresponding Buy or Sell parsed instruction is returned.
    In addition, the parser now also handles non conventional Raydium swap events.
    It will first look at instruction and inner instruction data (if available); if nothing
    is found there, it will examine log messages; and if still nothing is detected,
    it attempts to infer a likely swap from pre/post token balances if no explicit raydium swap event is found.
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
    ) -> ParsedInstruction:
        """
        Processes an unknown instruction. Scans inner instructions and top-level instructions
        for pump fun swap events, and now non conventional Raydium swap events.
        If an allowed unique pump fun swap event is found, returns a Buy or Sell instruction.
        Otherwise, attempts to detect a Raydium swap via its instruction format, log messages,
        or by inferring from pre/post token balances.
        Finally falls back to returning a generic Unknown instruction.
        """
        # ----- Pump Fun logic (existing) -----
        swap_events_inner = self._parse_swap_from_inner(tx)
        swap_events_instructions = self._parse_swap_from_instructions(tx)
        # Each item is a tuple: (swap_event, origin_program_id)
        swap_events = swap_events_inner + swap_events_instructions

        # Filter out events with blacklisted program IDs.
        allowed_events = [
            (swap, origin) for swap, origin in swap_events if origin not in BLACKLIST_PROGRAM_IDS
        ]

        # Build a unique set based on key parsed fields.
        unique_events = []
        seen = set()
        signature = tx.signatures[0]
        for swap, origin in allowed_events:
            try:
                is_buy = bool(self._get_field(swap, "is_buy"))
                user = str(self._get_field(swap, "user"))
                mint = str(self._get_field(swap, "mint"))
                sol_amount = int(self._get_field(swap, "sol_amount"))
                token_amount = int(self._get_field(swap, "token_amount"))
            except (ValueError, AttributeError):
                continue
            try:
                if is_buy:
                    # For buy: from_token = WSOL_MINT and to_token = mint.
                    from_token = WSOL_MINT
                    from_decimals = SOL_DECIMALS
                    to_token = mint
                    to_decimals = self._get_token_decimals(tx, mint)
                    instruction_name = "Buy"
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
            except (ValueError, AttributeError):
                # It's not a token, maybe an account
                continue
            
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

        # ----- Raydium non conventional swap detection -----
        # Attempt to extract Raydium swap events from instructions/inner instructions.
        raydium_events = self._parse_raydium_swap_from_instructions(tx) + self._parse_raydium_swap_from_inner(tx)
        unique_raydium_events = []
        seen_raydium = set()
        for decoded, idx in raydium_events:
            try:
                amount_in = int.from_bytes(decoded[1:9], byteorder="little", signed=False)
                minimum_amount_out = int.from_bytes(decoded[9:17], byteorder="little", signed=False)
            except Exception:
                continue
            key = (signature, idx, amount_in, minimum_amount_out)
            if key not in seen_raydium:
                seen_raydium.add(key)
                unique_raydium_events.append((decoded, idx))
        if unique_raydium_events:
            for decoded, idx in unique_raydium_events:
                try:
                    return self._build_raydium_swap(tx, decoded, idx)
                except Exception:
                    continue

        # If still not found, check for log messages hinting at a Raydium swap.
        if tx.meta and hasattr(tx.meta, "logMessages"):
            for log in tx.meta.logMessages:
                if "SwapRaydiumV4" in log:
                    try:
                        return self._build_raydium_swap(tx, b"", -1)
                    except Exception:
                        break

        # Finally, if no explicit event, attempt to infer a Raydium swap by comparing pre/post balances.
        try:
            inferred_swap = self._infer_raydium_swap(tx)
            if inferred_swap:
                return inferred_swap
        except Exception:
            pass

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
        with a pump fun or Raydium AMM program ID are considered.
        Returns a list of tuples containing the SwapData and the originating program id.
        """
        events: list[tuple[SwapData, str]] = []
        if not (tx.meta and hasattr(tx.meta, "innerInstructions")):
            return events

        for group in tx.meta.innerInstructions:
            instructions = group.get("instructions", [])
            for in_instr in instructions:
                sub_prog_id = tx.all_accounts[in_instr["programIdIndex"]]
                if sub_prog_id != PUMPFUN_PROGRAM_ID and sub_prog_id != RAYDIUM_AMM_PROGRAM_ID:
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
        with a pump fun or Raydium AMM program ID are considered.
        Returns a list of tuples containing the SwapData and its originating program id.
        """
        events: list[tuple[SwapData, str]] = []
        if not (tx.message and hasattr(tx.message, "instructions")):
            return events

        for instr in tx.message.instructions:
            sub_prog_id = tx.message.accountKeys[instr.programIdIndex]
            if sub_prog_id != PUMPFUN_PROGRAM_ID and sub_prog_id != RAYDIUM_AMM_PROGRAM_ID:
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

        pre_token_balance = 0
        pre_sol_balance = 0
        post_token_balance = 0
        post_sol_balance = 0

        for tb in tx.meta.preTokenBalances:
            if tb.mint == WSOL_MINT and tb.owner == who:
                pre_sol_balance = tb.uiTokenAmount.amount
            if tb.mint == to_token and tb.owner == who:
                pre_token_balance = tb.uiTokenAmount.amount

        for tb in tx.meta.postTokenBalances:
            if tb.mint == WSOL_MINT and tb.owner == who:
                post_sol_balance = tb.uiTokenAmount.amount
            if tb.mint == to_token and tb.owner == who:
                post_token_balance = tb.uiTokenAmount.amount

        return Buy(
            signature=tx.signatures[0],
            program_id=origin_program_id,
            program_name="PumpFun",
            instruction_name="Buy",
            who=who,
            from_token=from_token,
            from_token_decimals=from_decimals,
            to_token=to_token,
            to_token_decimals=to_decimals,
            from_token_amount=from_amount,
            to_token_amount=to_amount,
            pre_token_balance=pre_token_balance,
            post_token_balance=post_token_balance,
            pre_sol_balance=pre_sol_balance,
            post_sol_balance=post_sol_balance,
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

        pre_token_balance = 0
        pre_sol_balance = 0
        post_token_balance = 0
        post_sol_balance = 0

        for tb in tx.meta.preTokenBalances:
            if tb.mint == WSOL_MINT and tb.owner == who:
                pre_sol_balance = tb.uiTokenAmount.amount
            if tb.mint == from_token and tb.owner == who:
                pre_token_balance = tb.uiTokenAmount.amount

        for tb in tx.meta.postTokenBalances:
            if tb.mint == WSOL_MINT and tb.owner == who:
                post_sol_balance = tb.uiTokenAmount.amount
            if tb.mint == from_token and tb.owner == who:
                post_token_balance = tb.uiTokenAmount.amount
            
        return Sell(
            signature=tx.signatures[0],
            program_id=origin_program_id,
            program_name="PumpFun",
            instruction_name="Sell",
            who=who,
            from_token=from_token,
            from_token_decimals=from_decimals,
            to_token=to_token,
            to_token_decimals=to_decimals,
            from_token_amount=from_amount,
            to_token_amount=to_amount,
            pre_token_balance=pre_token_balance,
            post_token_balance=post_token_balance,
            pre_sol_balance=pre_sol_balance,
            post_sol_balance=post_sol_balance,
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

    # ---- New methods to handle non conventional Raydium swap events ----

    def _parse_raydium_swap_from_instructions(self, tx: Transaction) -> list[tuple[bytes, int]]:
        """
        Parses potential Raydium swap events from top-level instructions.
        Only instructions with the Raydium AMM program ID and a minimum expected payload length are considered.
        Returns a list of tuples: (decoded_data, instruction_index).
        """
        events: list[tuple[bytes, int]] = []
        if not (tx.message and hasattr(tx.message, "instructions")):
            return events

        for idx, instr in enumerate(tx.message.instructions):
            if tx.message.accountKeys[instr.programIdIndex] != RAYDIUM_AMM_PROGRAM_ID:
                continue
            try:
                raw_data = instr.data or ""
                try:
                    decoded_data = base58.decode(raw_data)
                except Exception:
                    decoded_data = base64.b64decode(raw_data)
            except Exception:
                continue
            if len(decoded_data) < 17:
                continue
            events.append((decoded_data, idx))
        return events

    def _parse_raydium_swap_from_inner(self, tx: Transaction) -> list[tuple[bytes, int]]:
        """
        Parses potential Raydium swap events from inner instructions.
        Only inner instructions with the Raydium AMM program ID and a minimal payload length are considered.
        Returns a list of tuples: (decoded_data, instruction_index) where instruction_index is taken from the group.
        """
        events: list[tuple[bytes, int]] = []
        if not (tx.meta and hasattr(tx.meta, "innerInstructions")):
            return events

        for group in tx.meta.innerInstructions:
            instructions = group.get("instructions", [])
            for in_instr in instructions:
                if tx.all_accounts[in_instr["programIdIndex"]] != RAYDIUM_AMM_PROGRAM_ID:
                    continue
                try:
                    raw_data = in_instr.get("data", "")
                    try:
                        decoded_data = base58.decode(raw_data)
                    except Exception:
                        decoded_data = base64.b64decode(raw_data)
                except Exception:
                    continue
                if len(decoded_data) < 17:
                    continue
                instr_index: int = group.get("index", -1)
                events.append((decoded_data, instr_index))
        return events

    def _build_raydium_swap(self, tx: Transaction, decoded_data: bytes, instruction_index: int) -> Swap:
        """
        Constructs a Raydium Swap parsed instruction from decoded instruction data.
        If the decoded_data is empty or no valid instruction exists (instruction_index < 0),
        falls back to inferring the swap from balances.
        """
        # If no explicit instruction data is available, use the inference method.
        if not decoded_data or instruction_index < 0:
            return self._infer_raydium_swap(tx)
        try:
            amount_in = int.from_bytes(decoded_data[1:9], byteorder="little", signed=False)
            minimum_amount_out = int.from_bytes(decoded_data[9:17], byteorder="little", signed=False)
        except Exception as e:
            raise ValueError("Invalid Raydium swap instruction data") from e

        # Attempt to retrieve the original instruction for account information.
        accounts: list[int] = []
        if tx.message and hasattr(tx.message, "instructions") and 0 <= instruction_index < len(tx.message.instructions):
            instr = tx.message.instructions[instruction_index]
            accounts = instr.accounts

        try:
            from soltxs.parser.parsers.raydiumAMM import WSOL_MINT as RA_WSOL_MINT, SOL_DECIMALS as RA_SOL_DECIMALS
        except Exception:
            RA_WSOL_MINT = WSOL_MINT
            RA_SOL_DECIMALS = SOL_DECIMALS

        from_token: str = RA_WSOL_MINT
        from_token_decimals: int = RA_SOL_DECIMALS
        to_token: str = RA_WSOL_MINT
        to_token_decimals: int = RA_SOL_DECIMALS

        if accounts and len(accounts) >= 3:
            try:
                user_source = tx.all_accounts[accounts[-3]]
                user_destination = tx.all_accounts[accounts[-2]]
                who = tx.all_accounts[accounts[-1]]
            except IndexError:
                who = tx.all_accounts[0]
                user_source = who
                user_destination = who
        else:
            who = tx.all_accounts[0] if tx.all_accounts else "unknown"
            user_source = who
            user_destination = who

        # --- Retrieve pre and post token balances using the source account index. ---
        if accounts and len(accounts) >= 3:
            source_account_index = accounts[-3]
        else:
            source_account_index = 0

        pre_token_balance = next(
            (int(tb.uiTokenAmount.amount) for tb in tx.meta.preTokenBalances if tb.accountIndex == source_account_index),
            None,
        )
        post_token_balance = next(
            (int(tb.uiTokenAmount.amount) for tb in tx.meta.postTokenBalances if tb.accountIndex == source_account_index),
            None,
        )

        # --- Retrieve SOL balances using the wallet account index (accounts[-1]). ---
        if accounts and len(accounts) >= 1:
            wallet_account_index = accounts[-1]
        else:
            wallet_account_index = 0

        pre_sol_balance = (
            tx.meta.preBalances[wallet_account_index]
            if wallet_account_index < len(tx.meta.preBalances)
            else None
        )
        post_sol_balance = (
            tx.meta.postBalances[wallet_account_index]
            if wallet_account_index < len(tx.meta.postBalances)
            else None
        )

        combined_tb = []
        if tx.meta:
            combined_tb.extend(tx.meta.preTokenBalances or [])
            combined_tb.extend(tx.meta.postTokenBalances or [])
        for tb in combined_tb:
            token_account = tx.all_accounts[tb.accountIndex]
            if token_account == user_source:
                from_token = tb.mint
                from_token_decimals = tb.uiTokenAmount.decimals
            elif token_account == user_destination:
                to_token = tb.mint
                to_token_decimals = tb.uiTokenAmount.decimals

        # --- Process inner instructions to capture transfer amounts ---
        to_token_amount: int = 0
        if tx.meta and tx.meta.innerInstructions:
            for i_group in tx.meta.innerInstructions:
                if i_group.get("index") == instruction_index:
                    for in_instr in i_group.get("instructions", []):
                        # Import TokenProgramParser dynamically for token transfer details.
                        from soltxs.parser.parsers.tokenProgram import TokenProgramParser
                        prog_id = tx.all_accounts[in_instr["programIdIndex"]]
                        if prog_id == TokenProgramParser.program_id:
                            action = TokenProgramParser.route_instruction(tx, in_instr)
                            if action.instruction_name in ["Transfer", "TransferChecked"] and getattr(action, "to", None) == user_destination:
                                to_token_amount = action.amount

        return Swap(
            signature=tx.signatures[0],
            program_id=RAYDIUM_AMM_PROGRAM_ID,
            program_name="RaydiumAMM",
            instruction_name="Swap",
            who=who,
            from_token=from_token,
            from_token_amount=amount_in,
            from_token_decimals=from_token_decimals,
            to_token=to_token,
            to_token_amount=to_token_amount,
            to_token_decimals=to_token_decimals,
            minimum_amount_out=minimum_amount_out,
            pre_token_balance=pre_token_balance,
            post_token_balance=post_token_balance,
            pre_sol_balance=pre_sol_balance,
            post_sol_balance=post_sol_balance,
        )

    def _infer_raydium_swap(self, tx: Transaction) -> Swap:
        """
        Infers a Raydium swap by comparing pre and post token balances.
        It searches for the token with the most significant decrease (assumed sold)
        and the token with the largest increase (assumed received).
        """
        if not (tx.meta and (tx.meta.preTokenBalances or tx.meta.postTokenBalances)):
            raise ValueError("Not enough token balance data to infer Raydium swap.")
        pre_balances = {tb.accountIndex: tb for tb in tx.meta.preTokenBalances or []}
        post_balances = {tb.accountIndex: tb for tb in tx.meta.postTokenBalances or []}

        drop_candidate = None
        drop_amount = 0
        increase_candidate = None
        increase_amount = 0

        for index, pre_tb in pre_balances.items():
            post_tb = post_balances.get(index)
            if post_tb is None:
                continue
            pre_amount = int(pre_tb.uiTokenAmount.amount)
            post_amount = int(post_tb.uiTokenAmount.amount)
            diff = pre_amount - post_amount
            if diff > drop_amount:
                drop_amount = diff
                drop_candidate = pre_tb

        for index, post_tb in post_balances.items():
            pre_tb = pre_balances.get(index)
            if pre_tb is None:
                continue
            pre_amount = int(pre_tb.uiTokenAmount.amount)
            post_amount = int(post_tb.uiTokenAmount.amount)
            diff = post_amount - pre_amount
            if diff > increase_amount:
                increase_amount = diff
                increase_candidate = post_tb

        if not drop_candidate or not increase_candidate:
            raise ValueError("Could not infer swap from balances.")

        try:
            from soltxs.parser.parsers.raydiumAMM import WSOL_MINT as RA_WSOL_MINT, SOL_DECIMALS as RA_SOL_DECIMALS, Swap
        except Exception:
            RA_WSOL_MINT = WSOL_MINT
            RA_SOL_DECIMALS = SOL_DECIMALS
            from soltxs.parser.parsers.raydiumAMM import Swap

        who: str = drop_candidate.owner

        # --- Retrieve token balances using drop_candidate's account index ---
        candidate_account_index = drop_candidate.accountIndex
        pre_token_balance = next(
            (int(tb.uiTokenAmount.amount) for tb in tx.meta.preTokenBalances if tb.accountIndex == candidate_account_index),
            None,
        )
        post_token_balance = next(
            (int(tb.uiTokenAmount.amount) for tb in tx.meta.postTokenBalances if tb.accountIndex == candidate_account_index),
            None,
        )

        # --- Retrieve SOL balances using the user's main wallet ---
        try:
            wallet_account_index = tx.all_accounts.index(who)
        except ValueError:
            wallet_account_index = 0

        pre_sol_balance = (
            tx.meta.preBalances[wallet_account_index]
            if wallet_account_index < len(tx.meta.preBalances)
            else None
        )
        post_sol_balance = (
            tx.meta.postBalances[wallet_account_index]
            if wallet_account_index < len(tx.meta.postBalances)
            else None
        )

        return Swap(
            signature=tx.signatures[0],
            program_id=RAYDIUM_AMM_PROGRAM_ID,
            program_name="RaydiumAMM",
            instruction_name="Swap",
            who=who,
            from_token=drop_candidate.mint,
            from_token_amount=drop_amount,
            from_token_decimals=drop_candidate.uiTokenAmount.decimals,
            to_token=increase_candidate.mint,
            to_token_amount=increase_amount,
            to_token_decimals=increase_candidate.uiTokenAmount.decimals,
            minimum_amount_out=0,
            pre_token_balance=pre_token_balance,
            post_token_balance=post_token_balance,
            pre_sol_balance=pre_sol_balance,
            post_sol_balance=post_sol_balance,
        )
