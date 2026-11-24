# Real-time Financial Transaction Anomaly Detection

## Set up 
0. Create `.env` file based on `.env.template`
1. Before running, you should have these files in `big-data-analytics/data/raw_kaggle/`: 
- `paysim_data.csv` 
- `train_identity.csv`
2. From the main directory run:
```bash
PYTHONPATH=. uv run generator/prepare_data.py
```
or for Windows PowerShell:
```bash
$env:PYTHONPATH="."
uv run generator/prepare_data.py
```
to create `parquet` files.

3. Run Docker deamon (open Docker app) and start containers: 
```bash
docker compose up -d
```
4. Run stream simulation by starting containers (you might do it in a separate terminal):
```bash
docker compose -f docker-compose-generator.yaml up -d
```
4. Check:
- if data is generated: logs of generator container
- if buckets were added to Minio: [http://localhost:9001](http://localhost:9001)
- if Airflow was initialized (might take a couple minutes due to downloading libraries): [http://localhost:8082](http://localhost:8082), click on DAGs to execute them 
- if Cassandra tables were created (about a minute after start): [http://localhost:3000](http://localhost:3000), keyspace `fraud_detection` should list 7 tables



## Services 
- Kafka: [http://localhost:8080](http://localhost:8080)
- Airflow: [http://localhost:8082](http://localhost:8082)
- Spark: [http://localhost:8083](http://localhost:8083)
- Silo (Minio): [http://localhost:9001](http://localhost:9001)
- Cassandra: [http://localhost:3000](http://localhost:3000)




