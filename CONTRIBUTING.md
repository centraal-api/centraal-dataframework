# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

## Types of Contributions

### Report Bugs

Report bugs at https://github.com/centraal-api/azure-datalake-utils/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

centraal_dataframework could always use more documentation, whether as part of the
official centraal_dataframework docs, in docstrings, or even on the web in blog posts,
articles, and such.

### Submit Feedback

The best way to send feedback is to file an issue at https://github.com/centraal-api/centraal_dataframework/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Get Started!

Ready to contribute? Here's how to set up `centraal_dataframework` for local development.

1. Fork the `centraal_dataframework` repo on GitHub.
2. Clone your fork locally

    ```
    $ git clone git@github.com:your_name_here/centraal-dataframework.git
    ```

3. Ensure [poetry](https://python-poetry.org/docs/) is installed.
4. Install dependencies and start your virtualenv:

    ```
    $ poetry install -E test -E doc -E dev
    ```

5. Create a branch for local development:

    ```
    $ git checkout -b name-of-your-bugfix-or-feature
    ```

    Now you can make your changes locally.

6. When you're done making changes, check that your changes pass the
   tests, including testing other Python versions, with tox:

    ```
    $ poetry run tox
    ```

7. Commit your changes and push your branch to GitHub:

    ```
    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature
    ```

8. Submit a pull request through the GitHub website.

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.md.
3. The pull request should work for Python 3.8, 3.9. 1.10 Check
   https://github.com/centraal-api/centraal-dataframework/actions
   and make sure that the tests pass for all supported Python versions.

## Tips

```
$ poetry run pytest tests/test_centraal_dataframework.py
```

To run a subset of tests.


## Deploying

A reminder for the maintainers on how to deploy.
Make sure all your changes are committed (including an entry in CHANGELOG.md).
Then run:

```
$ poetry run bump2version patch # possible: major / minor / patch
$ git push
$ git push --tags
```

GitHub Actions will then deploy to PyPI if tests pass.

### Note for maintaniners

In the first versions the dependencies are not stable, for 2 reasons:

- the versions suggested in the initial template were not fully compatible with the objectives of the library.

For the above reasons, in some cases, the release workflow could not work, and to avoid creating patch versions the maintainer may need to "recycle" a tag, please follow this gist:

```
git push origin :{tag}
git tag -d {tag}
git tag {tag}
git push origin {tag}
```

