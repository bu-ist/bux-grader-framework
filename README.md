# BU edX External Grader Framework

A simple framework for creating specialized [external graders](http://ca.readthedocs.org/en/latest/problems_tools/external_graders.html) for your [edX](http://code.edx.org/) course.

See the [demo course repository](https://github.com/bu-ist/bux-demo-course-grader) for an example course configuration. See the [configuration repository](configuration repository) for a more automated way to set up your grader environment.

## Prerequisites

* A dedicated XQueue queue with authentication credentials
* [RabbitMQ](http://www.rabbitmq.com/)
* [libxml2 and libxslt](http://lxml.de/installation.html)
* [statsd](https://github.com/etsy/statsd/) (optional)

## Contributing

Pull requests are welcome!

Pull down this repository and use `pip` to install development requirements:

```bash
$ git clone git@github.com:bu-ist/bux-grader-framework
$ pip install -r requirements.txt
```

### Documentation

Follow [pep257](http://legacy.python.org/dev/peps/pep-0257/)!

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

### Coding Style

Follow [pep8](http://legacy.python.org/dev/peps/pep-0008/)!

Run [flake8](https://pypi.python.org/pypi/flake8) before you commit to make sure there aren't any violations:

```bash
$ flake8
```
