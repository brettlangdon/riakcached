Riakcached
==========

A Memcached like interface to the Riak HTTP Client.


## Documentation
The documentation can be found in the `/docs` directory in this repository and should be fairly complete for the codebase.

### Building Documentation
To build the documentation yourself you will require `sphinx` and `sphinxcontrib-fulltoc`
#### Install Dependencies
```bash
pip install sphinx sphinxcontrib-fulltoc
```
#### Building
```bash
git clone git://github.com/brettlangdon/riakcached.git
cd ./docs
make html
```

## TODO
* Add Docstrings to methods
* Setup Travis CI
* Write a better README.md
