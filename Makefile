service := houzz-common-lib
.PHONY: upload

upload:
	python setup.py sdist
	-twine upload --repository pypi dist/*
	rm -rf dist