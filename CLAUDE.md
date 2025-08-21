# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow practice project using Python 3.13+. The project uses `uv` as the package manager and dependency resolver.

## Development Setup

- This project uses `uv` for dependency management
- Python version: >=3.13 (specified in pyproject.toml)
- Main entry point: `main.py`

## Common Commands

### Package Management
- Install dependencies: `uv sync`
- Add new dependency: `uv add <package>`
- Run Python scripts: `uv run python main.py`

### Docker
- Start services: `docker-compose up`
- Start in background: `docker-compose up -d`
- Stop services: `docker-compose down`

## Architecture

This is currently a minimal Python project with:
- Single entry point in `main.py`
- Docker Compose configuration for containerized development
- Standard Python project structure using pyproject.toml

The project appears to be in early setup phase for Airflow experimentation and learning.