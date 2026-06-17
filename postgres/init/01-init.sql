CREATE DATABASE prefect; -- Create the prefect database

\c tapdrink_telemetry; -- Connect to the tapdrink_telemetry database

CREATE TABLE IF NOT EXISTS realtime_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    purchase_count BIGINT,
    revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end)
);
