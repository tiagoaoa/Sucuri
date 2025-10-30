from __future__ import annotations

from pyDF.integrations import rust
from pyDF.plugins import lcs


@rust
def lcs_block(context, i, j, inputs):
    return lcs.block_python(context, i, j, inputs)


if __name__ == "__main__":
    raise SystemExit(lcs.cli_main(lcs_block))
