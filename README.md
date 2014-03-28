# BU edX External Grader Framework

A simple framework for creating specialized [external graders](http://ca.readthedocs.org/en/latest/problems_tools/external_graders.html) for your [edX](http://code.edx.org/) course.

## Contributing

Pull requests are welcome!

Pull down this repository and use `pip` to install development requirements:

```bash
$ git clone git@github.com:bu-ist/bux-grader-framework
$ pip install -r requirements.txt
```

### Documentation

Package documentation lives in the `docs` directory and can be built in a variety of formats using [sphinx](http://sphinx-doc.org/).

```bash
cd docs
make html
```

The build directory (`docs/_build`) is excluded from VCS.

### Tests

All unit tests live in the `tests` directory and can be run using [nose](https://nose.readthedocs.org/en/latest/).

```bash
$ nosetests
```
