.PHONY: install run demo spike burst overload metrics clean

install:
	pip install -r requirements.txt

run:
	./run.sh

demo:
	python3 -m loadgen.demo

spike:
	python3 -m loadgen.generate --profile spike --reset

burst:
	python3 -m loadgen.generate --concurrent 500 --reset

overload:
	python3 -m loadgen.generate --profile overload --reset

metrics:
	@curl -s localhost:8080/metrics/summary

clean:
	rm -rf logs __pycache__ */__pycache__
