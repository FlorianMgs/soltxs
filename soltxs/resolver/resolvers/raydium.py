from dataclasses import dataclass
from typing import List, Optional

from soltxs import parser
from soltxs.resolver.models import Resolve, Resolver

WSOL_MINT = "So11111111111111111111111111111111111111112"
SOL_MINT = "11111111111111111111111111111111"


@dataclass(slots=True)
class Raydium(Resolve):
    """
    Represents a resolved Raydium transaction.

    Attributes:
        type: The type of swap (e.g., 'swap', 'buy', 'sell').
        who: The user account performing the swap.
        from_token: The token being sold.
        from_amount: Amount sold (adjusted for decimals).
        to_token: The token being bought.
        to_amount: Amount bought (adjusted for decimals).
        minimum_amount_out: The minimum amount expected (adjusted for decimals).
    """

    type: str
    who: str
    from_token: str
    from_amount: int
    to_token: str
    to_amount: int
    minimum_amount_out: int
    signature: str
    pre_token_balance: int | None
    post_token_balance: int | None
    pre_sol_balance: int | None
    post_sol_balance: int | None


class _RaydiumResolver(Resolver):
    """
    Resolver for Raydium AMM instructions.
    """

    def resolve(self, instructions: List[parser.models.ParsedInstruction]) -> Optional[Resolve]:
        """
        Resolves Raydium swap instructions from parsed instructions.

        Args:
            instructions: List of parsed instructions.

        Returns:
            A Raydium resolved object if a swap instruction is found, else None.
        """
        instrs = [i for i in instructions if isinstance(i, parser.parsers.raydiumAMM.Swap)]
        if len(instrs) > 1:
            instrs = [i for i in instrs if i.to_token_amount > 0 and i.from_token_amount > 0]
        if len(instrs) == 1:
            instr = instrs[0]

            raydium_type = "swap"
            if instr.from_token in {WSOL_MINT, SOL_MINT}:
                raydium_type = "buy"
            elif instr.to_token in {WSOL_MINT, SOL_MINT}:
                raydium_type = "sell"

            token_decimals = 10**instr.to_token_decimals if raydium_type == "buy" else 10**instr.from_token_decimals

            return Raydium(
                type=raydium_type,
                who=instr.who,
                from_token=instr.from_token,
                from_amount=instr.from_token_amount / 10**instr.from_token_decimals,
                to_token=instr.to_token,
                to_amount=instr.to_token_amount / 10**instr.to_token_decimals,
                minimum_amount_out=instr.minimum_amount_out / 10**instr.to_token_decimals,
                signature=instr.signature,
                pre_token_balance=int(instr.pre_token_balance) / token_decimals if instr.pre_token_balance else None,
                post_token_balance=int(instr.post_token_balance) / token_decimals if instr.post_token_balance else None,
                pre_sol_balance=int(instr.pre_sol_balance) / 10**9 if instr.pre_sol_balance else None,
                post_sol_balance=int(instr.post_sol_balance) / 10**9 if instr.post_sol_balance else None,
            )


RaydiumResolver = _RaydiumResolver()
