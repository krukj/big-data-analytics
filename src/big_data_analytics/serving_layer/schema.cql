CREATE KEYSPACE IF NOT EXISTS fraud_detection
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE fraud_detection;


-- Real-time views

CREATE TABLE IF NOT EXISTS transactions_by_step (
    step              int,
    event_time        bigint,
    tx_id             timeuuid,
    second_offset     int,
    type              text,
    amount            double,
    name_orig         text,
    oldbalance_org    double,
    newbalance_orig   double,
    name_dest         text,
    oldbalance_dest   double,
    newbalance_dest   double,
    is_late           boolean,
    anomaly_score     double,
    is_anomaly        boolean,
    model_version     text,
    scored_at         timestamp,
    is_fraud          boolean,
    PRIMARY KEY ((step), event_time, tx_id)
) WITH CLUSTERING ORDER BY (event_time DESC, tx_id DESC);

CREATE TABLE IF NOT EXISTS alerts_by_step (
    step              int,
    anomaly_score     double,
    tx_id             timeuuid,
    event_time        bigint,
    type              text,
    amount            double,
    name_orig         text,
    name_dest         text,
    model_version     text,
    scored_at         timestamp,
    status            text,
    decided_at        timestamp,
    decided_by        text,
    PRIMARY KEY ((step), anomaly_score, tx_id)
) WITH CLUSTERING ORDER BY (anomaly_score DESC, tx_id DESC);

CREATE TABLE IF NOT EXISTS login_events_by_account (
    account_id        text,
    event_time        bigint,
    event_id          text,
    step              int,
    second_offset     int,
    event_type        text,
    device_id         text,
    device_type       text,
    operating_system  text,
    browser           text,
    device_info       text,
    screen_resolution text,
    PRIMARY KEY ((account_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);


-- Batch views

CREATE TABLE IF NOT EXISTS transaction_stats_by_step (
    day               int,
    step              int,
    tx_count          bigint,
    total_amount      double,
    avg_amount        double,
    max_amount        double,
    count_by_type     map<text, bigint>,
    amount_by_type    map<text, double>,
    computed_at       timestamp,
    PRIMARY KEY ((day), step)
) WITH CLUSTERING ORDER BY (step ASC);

CREATE TABLE IF NOT EXISTS recipient_profiles (
    name_dest         text,
    tx_count          bigint,
    total_amount      double,
    avg_amount        double,
    max_amount        double,
    distinct_senders  bigint,
    cash_out_count    bigint,
    transfer_count    bigint,
    first_step        int,
    last_step         int,
    computed_at       timestamp,
    PRIMARY KEY ((name_dest))
);


-- Analytical results

CREATE TABLE IF NOT EXISTS model_runs (
    model_name        text,
    trained_at        timestamp,
    model_version     text,
    train_step_from   int,
    train_step_to     int,
    train_rows        bigint,
    threshold         double,
    metrics           map<text, double>,
    artifact_path     text,
    PRIMARY KEY ((model_name), trained_at)
) WITH CLUSTERING ORDER BY (trained_at DESC);

CREATE TABLE IF NOT EXISTS anomaly_stats_by_step (
    day               int,
    step              int,
    model_version     text,
    n_scored          bigint,
    n_anomalies       bigint,
    anomaly_rate      double,
    score_p50         double,
    score_p99         double,
    n_fraud_labels    bigint,
    precision         double,
    recall            double,
    computed_at       timestamp,
    PRIMARY KEY ((day), step)
) WITH CLUSTERING ORDER BY (step ASC);
