.PHONY: lint run build bump-version

lint:
	python3 -m pylint src/presentations_module --disable=R,C --max-line-length=120

run:
	.venv/bin/python main.py

build:
	python3 -m build
