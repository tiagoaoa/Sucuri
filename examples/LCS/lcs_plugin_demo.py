from __future__ import annotations

from pyDF.integrations import rust
from pyDF.plugins import lcs


@rust
def lcs_block(context, i, j, inputs):
    north, west = lcs.unpack_inputs(i, j, inputs)
    start_a, end_a = context.bounds_a(j)
    start_b, end_b = context.bounds_b(i)
    width = end_a - start_a
    height = end_b - start_b

    matrix = [[0] * (width + 1) for _ in range(height + 1)]

    if north is not None:
        if len(north) != width + 1:
            raise ValueError("north border has invalid length")
        matrix[0] = lcs.copy_vector(north)
    if west is not None:
        if len(west) != height + 1:
            raise ValueError("west border has invalid length")
        for idx, value in enumerate(west):
            matrix[idx][0] = value

    seq_a = context.seq_a
    seq_b = context.seq_b

    for row in range(1, height + 1):
        ch_b = seq_b[start_b + row - 1]
        for col in range(1, width + 1):
            ch_a = seq_a[start_a + col - 1]
            if ch_a == ch_b:
                matrix[row][col] = matrix[row - 1][col - 1] + 1
            else:
                matrix[row][col] = max(matrix[row][col - 1], matrix[row - 1][col])

    bottom_row = matrix[-1]
    right_column = [matrix[row][width] for row in range(height + 1)]
    return bottom_row, right_column


if __name__ == "__main__":
    raise SystemExit(lcs.cli_main(lcs_block))
