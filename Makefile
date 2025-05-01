env:
	python3 -m venv .venv

install:
	 .venv/bin/pip install -r requirements.txt

run:
	.venv/bin/uvicorn server:app --host 0.0.0.0 --port 8000 &
	.venv/bin/uvicorn server:app --host 0.0.0.0 --port 8001 &
	.venv/bin/python3 client.py

kill:
	lsof -n -i4TCP:8000,8001 -sTCP:LISTEN -t | xargs kill -9