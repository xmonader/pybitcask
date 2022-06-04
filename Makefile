.PHONY: tests docs api_docs docs-serve

tests:
	pytest tests -s -vvv

integrationtests:
	pytest tests -sv -m "integration"

unittests:
	pytest tests -sv -m "unittests"

coverage:
	pytest tests -s --cov=pybitcask --cov-report=xml

api_docs:
	pdoc3 pybitcask --html --output-dir docs/api --force

docs: api_docs

docs-serve:
	python3 -m http.server --directory ./docs

requirements:
	 poetry export -f $@.txt --output $@.txt
