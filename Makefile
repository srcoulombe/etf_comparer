.PHONY: run run-container gcloud-deploy

run:
	@streamlit run etf_comparer.py --server.port=8080 --server.address=0.0.0.0

run-container:
	@docker build . -t etf_comparer.py
	@docker run -p 8080:8080 etf_comparer.py

gcloud-deploy:
	@gcloud app deploy app.yml
