# Contributing

Hello! Do you have a feature or bugfix you'd like to contribute? Amazing!
If your code change is small feel free to open a PR directly.
For larger proposed changes please either open a github issue or contact us in #folio-ldlite or ldlite-support@fivecolleges.edu.

## Dev Setup
This project has a number of "global" requirements to be developed on.
You can find them in [./environment.yaml](./environment.yaml) (which coincidentally is a conda environment definition).
You don't have to use conda, but they do have to be present.
Please check the document for the individual packages for installation instructions.

This project is using [PDM to manage its dependencies](https://pdm-project.org/en/latest/). You can install them with
```sh
pdm install -G:all --lockfile pylock.toml
```
Once you've installed the dependencies you should be able to edit the project using any modern python editor.

This project is using [precious to run code quality checks](https://github.com/houseabsolute/precious).
These checks are run when a Merge Request is opened. You can run them locally with `precious tidy` or `precious lint`.
Please consult the precious documentation for more information on running the checks defined in [./precious.toml](./precious.toml).

This project is using pytest and pytest-cases for "data-driven black-box" testing.
If you're having trouble with running the existing tests or adding new ones please reach out to the authors and we can help.
We'd like to keep the test coverage high but do not want to turn away contributions because of this high bar.

This project is using fairly strict settings for ruff and mypy.
If you're having trouble with the typing or linting please reach out to the authors and we can help.
We'd like to keep the codebase strict but do not want to turn away contributions because of this high bar.

## Opening a PR
When you open a PR a number of CI pipelines will run.
If any of these fail, please mark your PR as draft and work on fixing the issues.
You can recreate any of the CI checks locally using a combination precious and pytest.
When you are ready to re-run the CI checks you can mark your PR as ready for review.
Please assign @brycekbargar as a reviewer once all CI checks pass and you're ready for review.
