# Product Overview

## What is this?

A Rust CLI tool (`dagctl`) for interacting with the Dagster Cloud GraphQL API. It provides command-line access to query runs, events, logs, and code locations from a Dagster Cloud deployment.

## Target Users

Data engineers and platform teams who manage Dagster pipelines and need CLI-based access to run information for scripting, debugging, and automation.

## Core Capabilities

- **Run queries**: List runs (with optional limit), get run details, fetch run events, retrieve captured logs
- **Code locations**: List and inspect code locations with metadata, schedules, sensors, and jobs
- **Jobs**: List jobs across all code locations or filter by location, inspect job details including schedules, sensors, and tags
- **Assets**: List asset nodes with group and code location filters, inspect asset details including dependencies, dependents, owners, and jobs
- **Schema management**: Download the Dagster GraphQL schema for building from source
- **Authentication**: Token resolution via CLI flag → env var → config file (priority order)
- **Shell completion**: Generate completions for bash, zsh, fish, and PowerShell
- **Self-update**: Update to the latest release via `dagctl self update`
- **Output formats**: Table (default), JSON (`-o json`), and YAML (`-o yaml`)
- **Debug**: Print diagnostic info about API connectivity, version, and config
