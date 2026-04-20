# Databricks Medallion Architecture Portfolio

This repository includes a reusable Databricks medallion pipeline scaffold and now includes a **production-grade project blueprint for Mental Health Awareness, Early Detection, and Treatment Analytics**.

## Start Here

- Core pipeline scaffold: `notebooks/` + `src/` + `tests/`
- Mental health portfolio blueprint: `docs/mental_health_portfolio_project.md`
- Example Databricks workflow JSON: `docs/workflow_job.json`

## Tech Stack

- Databricks Workflows
- PySpark + Spark SQL
- Delta Lake and Medallion architecture
- Delta Live Tables (DLT) patterns
- MLflow for experiment tracking and model registry
- Unity Catalog for governance

## Quickstart

1. Run `notebooks/00_setup.py`.
2. Execute notebooks `01` through `04` in sequence.
3. Follow `docs/mental_health_portfolio_project.md` to adapt the scaffold into the mental-health domain.

## Why This Is Portfolio-Ready

The mental health project guide focuses on:
- business framing and measurable impact,
- end-to-end data + ML architecture,
- production controls (quality, governance, CI/CD, monitoring),
- and interview-ready storytelling.
