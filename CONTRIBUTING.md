# Contribution Guidelines

Great see that you are interested in contributing to this repository.

To make the process as smooth as possible for all involed parties, you will find some instructions
below.  In case there is some information missing, or you feel lost, do not
hesitate to ask or [open an issue](https://github.com/ai4copsec/collekt/issues/new)
 on this project.

 ## Installation

Follow the installation and setup instructions in the [README.md](README.md).


## Code and development style

In order to ensure correct format and linting, the repository uses a pre-commit hook.
The 'pre-commit' package is installed as part of the 'dev' dependencies and the configuration of the
package sits in [.pre-commit-config.yaml](.pre-commit-config.yaml). For supported hooks
visit [https://pre-commit.com/hooks.html](https://pre-commit.com/hooks.html).

Notebooks under `notebooks/` are stripped of their outputs, execution counts and
kernel metadata by [nbstripout](https://github.com/kynan/nbstripout), so commits
carry code only. Keep the `ruff-pre-commit` `rev` in step with the `ruff` pin in
`pyproject.toml`, or the hook and `just lint` will reformat each other's output.

Git hooks live in `.git/`, which is not tracked, so enable them once per clone
(`just install` does this for you):

```
    just hooks
```

Afterwards run the following command to find and fix issues across the repo:

```
    just hooks-all
```

A hook that rewrites a file (nbstripout, ruff-format) fails the commit on
purpose: re-`git add` the rewritten file and commit again.

In general please adhere to the existing code style and folder structuring.

Have a look at the existing module structure and consider where to put your additions to the project.
Are they domain specific, are there general changes? Will your change affect existing code.

In all cases add testcases under 'tests' in a module specific subfolder.
This project uses 'pytest'.
To get familiar with pytest (pytest --help or visit https://docs.pytest.org).
For a quick start, you can run:

```
    pytest
```

to run all tests.

Or:

```
    pytest tests/hozint-apiclient/core/test_config.py -k test_Report -s
```

to run a particular test case (-k) and redirecting the output to stdout (-s).



## Merge Requests

Please fork the repository and push your code change into a branch.
Then create a new merge request from this branch.

Generally, read through the documentation first to understand the main ideas and some underlying assumptions of this project.
If you intend to change some of the behavior, please try to validate by reading the documentation or opening an issue whether
there are reasons for a particular setup.
Some change might have minor or major and sometimes unforeseen side-effects.
So make sure, that in all cases you describe your intentions and arguments of a change in the merge request.

Otherwise, please ensure that:

1. your fork and changes are based on the repository's latest state of the 'main' branch.
1. `pytest tests` runs without any errors
1. `tox -e lint` runs without any errors


Again, if you require help with any of the above. Please contact the developers.
