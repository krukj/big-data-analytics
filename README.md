# Real-time Financial Transaction Anomaly Detection

## Set up 
0. Create `.env` file based on `.env.template`
1. Before running, you should have these files in `big-data-analytics/data/raw_kaggle/`: 
- `paysim_data.csv` 
- `train_identity.csv`
2. From the main directory run:
```bash
PYTHONPATH=. uv run src/big_data_analytics/generator/prepare_data.py
```
to create `parquet` files.

3. Run Docker deamon (open Docker app) and start containers: 
```bash
docker compose up -d
```
4. Check:
- if buckets were added to Minio: [http://localhost:9001](http://localhost:9001)
- if Airflow was initialized (might take a couple minutes due to downloading libraries): [http://localhost:8082](http://localhost:8082), click on DAGs to execute them 



## Services 
- Kafka: [http://localhost:8080](http://localhost:8080)
- Airflow: [http://localhost:8082](http://localhost:8082)
- Spark: [http://localhost:8083](http://localhost:8083)
- Silo (Minio): [http://localhost:9001](http://localhost:9001)




