# Product Overview

## What is this?

A Rust CLI tool (`dagster-cli`) for interacting with the Dagster Cloud GraphQL API. It provides command-line access to query runs, events, and logs from a Dagster Cloud deployment.

## Target Users

Data engineers and platform teams who manage Dagster pipelines and need CLI-based access to run information for scripting, debugging, and automation.

## Core Capabilities

- **Schema management**: Download the Dagster GraphQL schema via `cynic introspect`
- **Run queries**: List runs (with optional limit), get run details, fetch run events, retrieve captured logs
- **Authentication**: Token resolution via CLI flag → env var → config file (priority order)
- **JSON output**: All commands emit JSON to stdout; errors go to stderr with non-zero exit

## Current State

The CLI is functional with all planned run commands implemented. See `IMPLEMENTATION_STATUS.md` for details. Potential future work includes asset commands, job commands, pagination, and output formatting options.
