# Makefile for creating a new release of the package and uploading it to PyPI

help:
	@echo "Use 'make upload' to upload the package to PyPi"

upload:
	rm -r dist | true
	python -m build --sdist --wheel
	twine upload --skip-existing dist/*