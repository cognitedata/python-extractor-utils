Run the test suite based on `$ARGUMENTS`.

## Behavior

**No argument** → run all tests:
```
uv run pytest -v --show-capture=all --cov-report=term-missing --cov=cognite.extractorutils
```

**With argument** → treat `$ARGUMENTS` as an exact name and resolve it in this order:

1. **Folder**: check if a directory named `$ARGUMENTS` exists under `tests/` (e.g. `tests_unit`, `tests_integration`, `test_unstable`). If found, run that directory.
2. **File**: check if a file named `$ARGUMENTS` or `$ARGUMENTS.py` or `test_$ARGUMENTS.py` exists under `tests/`. If found, run that file.
3. **Test function**: grep for `def $ARGUMENTS` or `def test_$ARGUMENTS` across all test files. If found in exactly one place, run `<file>::<function>`.

**If nothing matches** → do not run pytest. Instead tell the user:
> No test found matching `$ARGUMENTS`. Check the folder name, file name, or test function name and try again.

**If multiple files/functions match** → do not run. List the matches and ask the user to be more specific.

Always run with `-v --show-capture=all`. Report passed/failed/skipped counts. For failures show file, line number, and what went wrong.
