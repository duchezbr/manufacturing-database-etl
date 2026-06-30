# Manufacturing Database ETL with DuckDB & Python

> **🚧 Work in Progress**  
> This project is under active development. The database design, ETL process, and documentation will continue to evolve as new functionality is implemented.

## Problem Statement

Manufacturing process data is often exchanged as recurring flat files from multiple sources. While suitable for data transfer, these files are difficult to maintain over time, enforce data integrity, and support scalable analytics.

This project explores the design of a normalized relational database and ETL process that transforms flat-file manufacturing data into a centralized, structured data store.

## Overview

This repository demonstrates the development of a normalized manufacturing database using **DuckDB** and **Python**. The project focuses on relational database design and ETL techniques for loading and maintaining manufacturing process data.

## Current Features

- DuckDB database initialization
- Normalized relational schema
- Initial data migration from CSV
- Primary and foreign key relationships
- Mock manufacturing dataset for development and testing

## Database Model

The current schema includes the following entities:

- Manufacturer
- Process
- Unit Operation
- Parameter
- Batch
- Result

## Technology Stack

- Python
- DuckDB
- Pandas
- Jupyter Notebook

## Roadmap

Planned enhancements include:

- Incremental ETL process for recurring data loads
- Data validation and error handling

## Purpose

This project serves as a portfolio example demonstrating:

- Relational database design
- Data modeling
- ETL development
- Python data engineering
- Manufacturing data management
