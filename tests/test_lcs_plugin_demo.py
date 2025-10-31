from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path

from examples.LCS import lcs_integration as lcs
from examples.LCS import lcs_plugin_demo


class LcsPluginDemoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        crate_dir = repo_root / "rust" / "sucuri_lcs"
        target_dir = crate_dir / "target" / "release"

        env = os.environ.copy()
        env.setdefault("CARGO_TERM_COLOR", "never")
        env.setdefault("PYO3_USE_ABI3_FORWARD_COMPATIBILITY", "1")
        subprocess.run(
            ["cargo", "build", "--release"],
            check=True,
            cwd=str(crate_dir),
            env=env,
        )

        os.environ.setdefault("SUCURI_RUST_PATH", str(target_dir))
        if str(target_dir) not in sys.path:
            sys.path.insert(0, str(target_dir))

        cls.context = lcs.LCSContext.from_strings("ACGTACGT", "TACGACGA", block=4)

    def test_decorator_marks_function(self) -> None:
        self.assertTrue(getattr(lcs_plugin_demo.lcs_block, "__rust__", False))

    def test_rust_and_python_paths_agree(self) -> None:
        python_score = lcs.compute(
            self.context,
            workers=2,
            compute_fn=lcs_plugin_demo.lcs_block.__python_impl__,
        )
        rust_score = lcs.compute(self.context, workers=2, compute_fn=lcs_plugin_demo.lcs_block)
        self.assertEqual(python_score, rust_score)


if __name__ == "__main__":
    unittest.main()
