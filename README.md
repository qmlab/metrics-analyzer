# Metrics Analyzer

This is a prototype using InfluxDB to do query and subquery analysis for MP query execution metrics.
It collects data into a TSDB and performs analysis。

## Components

1.main - entry point of the command
2.config - configuration
3.data - data models of query and datapoint
4.ingress - data collector and DB data loader
5.analysis - data analyzers
