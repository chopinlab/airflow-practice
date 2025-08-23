# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow practice project using Python 3.12+. The project uses `uv` as the package manager and dependency resolver.

## Development Setup

- This project uses `uv` for dependency management
- Python version: >=3.12 (specified in pyproject.toml)
- Main DAG entry point: `dags/basic_dag_example.py`
- Uses TaskFlow API (@dag decorator) for modern Airflow development

## Common Commands

### Package Management
- Install dependencies: `uv sync`
- Add new dependency: `uv add <package>`
- Add dev dependency: `uv add --dev <package>`
- Format code: `uv run black .`
- Check formatting: `uv run black --check .`

### Docker
- Start services: `docker-compose up`
- Start in background: `docker-compose up -d`
- Stop services: `docker-compose down`

## Architecture

This is an Apache Airflow practice project with:
- DAG definitions in `dags/` directory
- TaskFlow API implementation using @dag and @task decorators
- Docker Compose configuration for containerized Airflow development
- Standard Python project structure using pyproject.toml
- Black formatter for code styling

## DAG Development

- Primary DAG: `dags/basic_dag_example.py`
- Uses TaskFlow API (@dag decorator) for modern Airflow development
- Includes ETL pipeline example (extract, transform, load tasks)
- Task documentation using docstrings for Airflow UI integration
- UTC timezone usage with pendulum for consistent scheduling