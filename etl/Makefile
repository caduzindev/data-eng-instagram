processor_batch_service:
	./processor_batch/venv/bin/uvicorn processor_batch.main:app --reload --reload-exclude "load_raw_data/*"

load_raw_data_service:
	./load_raw_data/venv/bin/python -m load_raw_data.main