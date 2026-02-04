.PHONY: lint run build

lint:
	pylint src/presentations_module --disable=R,C --max-line-length=120

run:
	python main.py

build:
	python3 -m build
