deploy:
	databricks bundle validate -t dev --profile Hasan

	databricks bundle deploy -t dev --profile Hasan

	databricks bundle run orders_pipeline -t dev --profile Hasan