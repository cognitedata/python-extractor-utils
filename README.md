<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Python Package Template
=======================
[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/python-package-template/master)](https://jenkins.cognite.ai/job/github-builds/job/python-package-template/job/master/)
[![codecov](https://codecov.io/gh/cognitedata/python-package-template/branch/master/graph/badge.svg)](https://codecov.io/gh/cognitedata/python-package-template)

Template for Cognite Python packages

## Setup
This package setup includes the following

- Dependency & Virtual Environment management with Pipenv
- Jenkins build with automated tests, code style checks, and release to PyPi
- Testing with tox to test against multiple versions of Python
- Documentation with Sphinx and www.readthedocs.com
- Package namespace under `cognite`
- Pre-commit hooks enforcing code style
- Changelog template

To activate the venv, install dependencies, and install pre-commit hooks, run the following commands in the root directory
```
$ pip install pipenv
$ pipenv shell
$ pipenv sync -d
$ pre-commit install
```

## PyPi
This setup will release your package to the public package index PyPi if you bump the version number and push to master.
You only have to uncomment the last part of the Jenkinsfile:
```
// if (env.BRANCH_NAME == 'master' && currentVersion != pipVersion) {
//    stage('Release') {
//        sh("pipenv run twine upload --config-file /pypi/.pypirc dist/*")
//    }
//}
```
If you want to release your package on artifactory instead, include `-r artifactory` like this:
```
pipenv run twine upload --config-file /pypi/.pypirc -r artifactory dist/*
```

## Versioning
All Cognite Python packages should adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Namespace
This setup will create your package under the `cognite` namespace.
NB! Do not create an `__init__.py` file in the `cognite` directory.

## Dependencies
Package dependencies should be declared in the `install_requires` section of `setup.py`. 
The Pipfile is only for development purposes.

## Coverage
Set up codecov.io by following [these](https://cognitedata.atlassian.net/wiki/spaces/COG/pages/111444069/Code+coverage) instructions:


## Docs
Generate docs locally
```
$ cd docs
$ make html
```
Set up readthedocs.com webhook to publish documentation when pushing to master.

## Tests
Run the following command in the root directory
```
$ pytest --cov
```
or to run against python version 3.5, 3.6 and 3.7
```
tox -p all
```

## License
Cognite Packages use the Apache 2.0 License. See [LICENSE](LICENSE)
