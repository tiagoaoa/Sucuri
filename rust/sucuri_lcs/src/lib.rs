use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyfunction]
#[pyo3(signature = (
    seq_a,
    seq_b,
    start_a,
    end_a,
    start_b,
    end_b,
    north=None,
    west=None
))]
fn lcs_block<'py>(
    py: Python<'py>,
    seq_a: Bound<'py, PyBytes>,
    seq_b: Bound<'py, PyBytes>,
    start_a: usize,
    end_a: usize,
    start_b: usize,
    end_b: usize,
    north: Option<Vec<i32>>,
    west: Option<Vec<i32>>,
) -> PyResult<(Vec<i32>, Vec<i32>)> {
    let slice_a = seq_a.as_bytes();
    let slice_b = seq_b.as_bytes();

    if end_a < start_a || end_a > slice_a.len() {
        return Err(PyValueError::new_err("invalid range for sequence A"));
    }
    if end_b < start_b || end_b > slice_b.len() {
        return Err(PyValueError::new_err("invalid range for sequence B"));
    }

    let block_a = &slice_a[start_a..end_a];
    let block_b = &slice_b[start_b..end_b];

    let result = py.allow_threads(|| lcs_block_inner(block_a, block_b, north.as_ref(), west.as_ref()));
    result
}

fn lcs_block_inner(
    block_a: &[u8],
    block_b: &[u8],
    north: Option<&Vec<i32>>,
    west: Option<&Vec<i32>>,
) -> PyResult<(Vec<i32>, Vec<i32>)> {
    let width = block_a.len();
    let height = block_b.len();

    let mut prev_row = match north {
        Some(values) => {
            if values.len() != width + 1 {
                return Err(PyValueError::new_err("north border has invalid length"));
            }
            values.clone()
        }
        None => vec![0; width + 1],
    };

    let mut last_column = match west {
        Some(values) => {
            if values.len() != height + 1 {
                return Err(PyValueError::new_err("west border has invalid length"));
            }
            values.clone()
        }
        None => vec![0; height + 1],
    };

    // Ensure the first element matches the border (or zero when absent).
    if let Some(values) = west {
        prev_row[0] = values[0];
    } else {
        prev_row[0] = 0;
        last_column[0] = 0;
    }

    let mut current_row = vec![0_i32; width + 1];

    for (row_idx, &ch_b) in block_b.iter().enumerate() {
        current_row[0] = match west {
            Some(values) => values[row_idx + 1],
            None => 0,
        };

        for (col_idx, &ch_a) in block_a.iter().enumerate() {
            if ch_a == ch_b {
                current_row[col_idx + 1] = prev_row[col_idx] + 1;
            } else {
                current_row[col_idx + 1] = prev_row[col_idx + 1].max(current_row[col_idx]);
            }
        }

        last_column[row_idx + 1] = current_row[width];
        prev_row.copy_from_slice(&current_row);
    }

    Ok((prev_row, last_column))
}

#[pymodule]
fn sucuri_lcs(py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(lcs_block, module)?)?;

    // Allow `help(sucuri_lcs)` to surface the docstring.
    let doc = "Accelerated LCS block computation for the Sucuri dataflow examples.";
    module.add("__doc__", doc)?;

    // Silence unused variable warning for `py`.
    let _ = py;
    Ok(())
}
