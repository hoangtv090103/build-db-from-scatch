# Build a DB from scratch with Python

## Objectives

- Build a system to store data relationally (table, row, column)
- Basic operations: CRUD, indexing
- Create index for one or more columns
- Store data persistently into files
- Create index for column(s)
- Understand the schema and how it operates

## Design

1. Storage Layer: Manage storing data
2. Query Layer: Process CRUD
3. Indexing Layer: Boose query performcance

## 1. Research and preparation

### Objectives

- Understand the core concepts of databases, DBMS, and indexing.
- Learn how Python handles file operations and data structures relevant to building a database.
- Decide on a storage format for your database.

### Study DB Basics

- What to learn:
    - Database: A structured collection of data, typically stored in tables with rows (records) and columns (attributes).
    - Table: A collection of records, where each record has values for predefined columns (e.g., a "Students" table with columns "ID", "Name", "Age").
    - DBMS: Software that manages databases, handling tasks like data storage, retrieval, updates, and indexing (e.g., MySQL, SQLite).
    - Primary Key: A unique identifier for each record in a table (e.g., "ID" in a Students table).
    - Query: A command to interact with the database (e.g., insert, select, update, delete).
    
### Learn about indexing
- What to learn
    - Indexing: A technique to speed up data retrieval by creating a data structure (index) that maps column values to record locations.
    - Hash Index: Uses a hash table (like a Python dictionary) to map values to records. Fast for exact matches (e.g., "Name = 'Alice'") but not for range queries.
    - B-tree Index: A tree-based structure that supports both exact matches and range queries (e.g., "Age > 20"). Common in real DBMSs like PostgreSQL.
# build-db-from-scatch
