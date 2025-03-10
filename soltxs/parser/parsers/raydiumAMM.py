from dataclasses import dataclass
from typing import Union

import qbase58 as base58
import base64

from soltxs.normalizer.models import Instruction, Transaction
from soltxs.parser.models import ParsedInstruction, Program
from soltxs.parser.parsers.tokenProgram import TokenProgramParser

WSOL_MINT = "So11111111111111111111111111111111111111112"
SOL_DECIMALS = 9


@dataclass(slots=True)
class Swap(ParsedInstruction):
    """
    Parsed instruction for a Raydium AMM swap.

    Attributes:
        who: The user performing the swap.
        from_token: The token being swapped from.
        from_token_amount: Raw amount of the from token.
        from_token_decimals: Decimals for the from token.
        to_token: The token being swapped to.
        to_token_amount: Raw amount of the to token.
        to_token_decimals: Decimals for the to token.
        minimum_amount_out: Minimum amount expected from the swap.
        pre_token_balance: User's token account balance before the swap.
        post_token_balance: User's token account balance after the swap.
        pre_sol_balance: User's SOL balance before the swap.
        post_sol_balance: User's SOL balance after the swap.
    """

    who: str
    from_token: str
    from_token_amount: int
    from_token_decimals: int
    to_token: str
    to_token_amount: int
    to_token_decimals: int
    minimum_amount_out: int
    signature: str
    pre_token_balance: int | None
    post_token_balance: int | None
    pre_sol_balance: int | None
    post_sol_balance: int | None

ParsedInstructions = Union[Swap]


class _RaydiumAMMParser(Program[ParsedInstructions]):
    """
    Parser for Raydium AMM v4 token swap instructions.
    """

    def __init__(self):
        self.program_id = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
        self.program_name = "RaydiumAMM"
        # Use the first byte of the decoded data as the discriminator.
        self.desc = lambda d: d[0]
        self.desc_map = {9: self.process_Swap}

    def process_Swap(
        self,
        tx: Transaction,
        instruction_index: int,
        decoded_data: bytes,
    ) -> Swap:
        """
        Processes a Swap instruction.

        Args:
            tx: The Transaction object.
            instruction_index: The index of the instruction.
            decoded_data: Decoded instruction data (re-decoded from instruction data).

        Returns:
            A Swap parsed instruction.
        """
        # Retrieve the original instruction.
        instr: Instruction = tx.message.instructions[instruction_index]
        accounts = instr.accounts

        # Re-decode the instruction data (to ensure consistency) using base58.
        try:
            decoded_data = base58.decode(instr.data or "")
        except ValueError:
            decoded_data = base64.b64decode(instr.data or "")

        # Extract the input amount and minimum output amount.
        amount_in = int.from_bytes(decoded_data[1:9], byteorder="little", signed=False)
        minimum_amount_out = int.from_bytes(decoded_data[9:17], byteorder="little", signed=False)

        # Identify user accounts based on known positions.
        user_source = tx.all_accounts[accounts[-3]]
        user_destination = tx.all_accounts[accounts[-2]]
        who = tx.all_accounts[accounts[-1]]

        # Default token info (assumed to be SOL/WSOL).
        from_token = WSOL_MINT
        from_token_decimals = SOL_DECIMALS
        to_token = WSOL_MINT
        to_token_decimals = SOL_DECIMALS

        # --- Retrieve pre and post balances ---

        # For the token, we take the 'source' account (accounts[-3]) as the one to track.
        source_account_index = accounts[-3]
        pre_token_balance = next(
            (int(tb.uiTokenAmount.amount) for tb in tx.meta.preTokenBalances if tb.accountIndex == source_account_index),
            None,
        )
        post_token_balance = next(
            (int(tb.uiTokenAmount.amount) for tb in tx.meta.postTokenBalances if tb.accountIndex == source_account_index),
            None,
        )

        # For SOL, we use the main user wallet ('who', accounts[-1]). We assume that the
        # preBalances and postBalances lists in tx.meta align with tx.all_accounts.
        wallet_account_index = accounts[-1]
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

        # Consolidate token balances from pre and post balances to update token mint and decimals.
        combined_tb = []
        combined_tb.extend(tx.meta.preTokenBalances)
        combined_tb.extend(tx.meta.postTokenBalances)

        for tb in combined_tb:
            token_account = tx.all_accounts[tb.accountIndex]
            if token_account == user_source:
                from_token = tb.mint
                from_token_decimals = tb.uiTokenAmount.decimals
            elif token_account == user_destination:
                to_token = tb.mint
                to_token_decimals = tb.uiTokenAmount.decimals

        # --- Process inner instructions to capture transfer amounts ---
        to_token_amount = 0
        inner_instrs = []
        # Find inner instructions corresponding to this instruction index.
        for i_group in tx.meta.innerInstructions:
            if i_group.get("index") == instruction_index:
                inner_instrs.extend(i_group["instructions"])
                break

        for in_instr in inner_instrs:
            prog_id = tx.all_accounts[in_instr["programIdIndex"]]
            if prog_id == TokenProgramParser.program_id:
                action = TokenProgramParser.route_instruction(tx, in_instr)
                if action.instruction_name in ["Transfer", "TransferChecked"] and action.to == user_destination:
                    to_token_amount = action.amount

        # --- Additional logic for sell transactions ---
        if to_token == WSOL_MINT and to_token_amount == 0:
            # First attempt: use preTokenBalances and postTokenBalances to compute the delta.
            candidate_amount: int = 0
            candidate_decimals: int = SOL_DECIMALS

            # Build a mapping of WSOL amounts from postTokenBalances keyed by accountIndex.
            post_wsol: dict[int, int] = {}
            for tb in tx.meta.postTokenBalances:
                if tb.mint == WSOL_MINT and tb.uiTokenAmount and tb.uiTokenAmount.amount:
                    post_wsol[tb.accountIndex] = int(tb.uiTokenAmount.amount)

            for tb in tx.meta.preTokenBalances:
                if tb.mint == WSOL_MINT and tb.uiTokenAmount and tb.uiTokenAmount.amount:
                    pre_amount = int(tb.uiTokenAmount.amount)
                    post_amount = post_wsol.get(tb.accountIndex, pre_amount)
                    delta = pre_amount - post_amount
                    if delta > candidate_amount:
                        candidate_amount = delta
                        candidate_decimals = tb.uiTokenAmount.decimals

            if candidate_amount > 0:
                to_token_amount = candidate_amount
                to_token_decimals = candidate_decimals
            else:
                # Fallback: Attempt to decode ray logs.
                for log in tx.meta.logMessages:
                    if "ray_log:" in log:
                        try:
                            raw_log = log.split("ray_log:")[1].strip()
                            decoded_log = base58.decode(raw_log)
                            # If the ray log follows a similar structure, e.g.,
                            # [discriminator (1 byte)] + [unused (8 bytes)] + [amount (8 bytes)]
                            if len(decoded_log) >= 17:
                                candidate_amount = int.from_bytes(decoded_log[9:17], byteorder="little", signed=False)
                                if candidate_amount > 0:
                                    to_token_amount = candidate_amount
                                    break
                        except Exception:
                            continue

        return Swap(
            program_id=self.program_id,
            program_name=self.program_name,
            instruction_name="Swap",
            who=who,
            from_token=from_token,
            from_token_amount=amount_in,
            from_token_decimals=from_token_decimals,
            to_token=to_token,
            to_token_amount=to_token_amount,
            to_token_decimals=to_token_decimals,
            minimum_amount_out=minimum_amount_out,
            signature=tx.signatures[0],
            pre_token_balance=pre_token_balance,
            post_token_balance=post_token_balance,
            pre_sol_balance=pre_sol_balance,
            post_sol_balance=post_sol_balance,
        )


RaydiumAMMParser = _RaydiumAMMParser()
