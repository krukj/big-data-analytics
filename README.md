# big-data-analytics

- generator - loads data / generates data and publishes to topics in kafka
- batch layer - trains model on historic data (stored in minio / s3)
- speed layer - kafka consumer, transforms data, uses ml model to predict y
- dashboard - stats etc