import hashlib
from dataclasses import dataclass
from typing import Union, List

import qbase58 as base58
import qborsh
import base64

from soltxs.normalizer.models import Instruction, Transaction
from soltxs.parser.models import ParsedInstruction, Program

WSOL_MINT = "So11111111111111111111111111111111111111112"
SOL_DECIMALS = 9
# Allowed inner instruction program id from pump fun operations.
PUMPFUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

@qborsh.schema
class SwapData:
    """
    Minimal Borsh schema for swap data in Mortem operations.
    (This should match the actual schema used in your swap operations.)

    Attributes:
        mint: Token mint address (the token being bought or sold).
        sol_amount: Amount in SOL.
        token_amount: Amount of token.
        is_buy: True for a buy operation, False for a sell.
        user: The user's account address.
    """
    mint: qborsh.PubKey
    sol_amount: qborsh.U64
    token_amount: qborsh.U64
    is_buy: qborsh.Bool
    user: qborsh.PubKey

@dataclass(slots=True)
class Buy(ParsedInstruction):
    """
    Parsed instruction for a Mortem 'Buy' operation.

    Attributes:
        who: Buyer's account.
        from_token: The token paid (SOL).
        from_token_decimals: Decimals for SOL.
        to_token: The token being bought.
        to_token_decimals: Decimals for the bought token.
        from_token_amount: Amount of SOL transferred.
        to_token_amount: Amount of token purchased.
    """
    signature: str
    who: str
    from_token: str
    from_token_decimals: int
    to_token: str
    to_token_decimals: int
    from_token_amount: int
    to_token_amount: int

@dataclass(slots=True)
class Sell(ParsedInstruction):
    """
    Parsed instruction for a Mortem 'Sell' operation.

    Attributes:
        who: Seller's account.
        from_token: The token being sold.
        from_token_decimals: Decimals for the sold token.
        to_token: The token received (SOL).
        to_token_decimals: Decimals for SOL.
        from_token_amount: Amount of token sold.
        to_token_amount: Amount of SOL received.
    """
    signature: str
    who: str
    from_token: str
    from_token_decimals: int
    to_token: str
    to_token_decimals: int
    from_token_amount: int
    to_token_amount: int

ParsedInstructions = Union[Buy, Sell]

class _MortemParser(Program[ParsedInstructions]):
    program_id = "FAdo9NCw1ssek6Z6yeWzWjhLVsr8uiCwcWNUnKgzTnHe"
    program_name = "Mortem"
    
    def desc(self, data: bytes) -> bytes:
        return data[:4]

    # The routing map now includes a default handler that checks 
    # for buy swap data first and then falling back to sell.
    desc_map = {
        b"buy\x00": lambda tx, idx, data: _MortemParser().parse_buy(tx, idx, data),
        b"sell": lambda tx, idx, data: _MortemParser().parse_sell(tx, idx, data),
        "default": lambda tx, idx, data: _MortemParser().parse_default(tx, idx, data),
    }

    def parse_buy(self, tx: Transaction, instruction_index: int, decoded_data: bytes) -> Buy:
        swap_list: List = self._parse_swap(tx, instruction_index)
        buy_data = None
        for swap in swap_list:
            if self._get_field(swap, "is_buy"):
                buy_data = swap
                break
        if buy_data is None:
            raise ValueError("No buy swap data found in inner instructions")
        who = str(self._get_field(buy_data, "user"))
        from_token = WSOL_MINT
        to_token = str(self._get_field(buy_data, "mint"))
        from_amount = int(self._get_field(buy_data, "sol_amount"))
        to_amount = int(self._get_field(buy_data, "token_amount"))
        from_decimals = SOL_DECIMALS
        to_decimals = self._get_token_decimals(tx, to_token)
        return Buy(
            signature=tx.signatures[0],
            program_id=self.program_id,
            program_name=self.program_name,
            instruction_name="Buy",
            who=who,
            from_token=from_token,
            from_token_decimals=from_decimals,
            to_token=to_token,
            to_token_decimals=to_decimals,
            from_token_amount=from_amount,
            to_token_amount=to_amount,
        )

    def parse_sell(self, tx: Transaction, instruction_index: int, decoded_data: bytes) -> Sell:
        swap_list: List = self._parse_swap(tx, instruction_index)
        sell_data = None
        for swap in swap_list:
            if not self._get_field(swap, "is_buy"):
                sell_data = swap
                break
        if sell_data is None:
            raise ValueError("No sell swap data found in inner instructions")
        who = str(self._get_field(sell_data, "user"))
        from_token = str(self._get_field(sell_data, "mint"))
        to_token = WSOL_MINT 
        from_amount = int(self._get_field(sell_data, "token_amount"))
        to_amount = int(self._get_field(sell_data, "sol_amount"))
        from_decimals = self._get_token_decimals(tx, from_token)
        to_decimals = SOL_DECIMALS
        return Sell(
            signature=tx.signatures[0],
            program_id=self.program_id,
            program_name=self.program_name,
            instruction_name="Sell",
            who=who,
            from_token=from_token,
            from_token_decimals=from_decimals,
            to_token=to_token,
            to_token_decimals=to_decimals,
            from_token_amount=from_amount,
            to_token_amount=to_amount,
        )

    def parse_default(self, tx: Transaction, instruction_index: int, decoded_data: bytes) -> ParsedInstructions:
        """
        Unified parser for both buy and sell. It checks the inner instructions for
        swap data first and returns a buy if any swap is flagged as is_buy. Otherwise,
        it falls back to a sell operation.
        """
        swap_list: List = self._parse_swap(tx, instruction_index)
        for swap in swap_list:
            if self._get_field(swap, "is_buy"):
                return self.parse_buy(tx, instruction_index, decoded_data)
        for swap in swap_list:
            if not self._get_field(swap, "is_buy"):
                return self.parse_sell(tx, instruction_index, decoded_data)
        raise ValueError("No valid swap data found in inner instructions")

    def _parse_swap(self, tx: Transaction, instruction_index: int) -> List:
        """
        Parses swap data from inner instructions for the given Mortem instruction.
        Only inner instructions from Mortem or PumpFun are considered.
        """
        allowed_program_ids = {self.program_id, PUMPFUN_PROGRAM_ID}
        inner_instrs = []
        for group in tx.meta.innerInstructions:
            if group.get("index") == instruction_index:
                inner_instrs.extend(group["instructions"])
        result_list: List = []
        for in_instr in inner_instrs:
            sub_prog_id = tx.all_accounts[in_instr["programIdIndex"]]
            if sub_prog_id not in allowed_program_ids:
                continue
            try:
                raw_data = base58.decode(in_instr.get("data", ""))
            except Exception:
                try:
                    raw_data = base64.b64decode(in_instr.get("data", ""))
                except Exception:
                    continue
            # Expect at least 16 bytes to skip and valid swap payload.
            if len(raw_data) < 48:
                continue
            swap_raw = raw_data[16:]
            try:
                swap_obj = SwapData.decode(swap_raw)
            except Exception:
                continue
            result_list.append(swap_obj)
        return result_list

    def _get_token_decimals(self, tx: Transaction, mint: str) -> int:
        """
        Retrieves the decimals for a given mint from token balances.
        """
        if mint == WSOL_MINT:
            return SOL_DECIMALS
        for tb in tx.meta.preTokenBalances + tx.meta.postTokenBalances:
            if tb.mint == mint:
                return tb.uiTokenAmount.decimals
        raise ValueError(f"Could not find decimals for mint {mint}")

    def _get_field(self, data, field: str):
        """
        Helper method to retrieve a field from swap data.
        Works whether data is returned as a dict or an object.
        """
        if isinstance(data, dict):
            return data.get(field)
        return getattr(data, field)

MortemParser = _MortemParser()