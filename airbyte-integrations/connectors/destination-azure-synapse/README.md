# Azure Synapse Analytics Destination

This is the Airbyte destination connector for Azure Synapse Analytics (dedicated SQL pool).

## Overview

Azure Synapse Analytics is a limitless analytics service that brings together data integration, enterprise data warehousing, and big data analytics. This connector supports loading data into Azure Synapse dedicated SQL pools.

## Key Features

- **Two loading modes**: INSERT (row-by-row) and COPY INTO (high-performance bulk loading via Azure Blob Storage)
- **COPY INTO**: The recommended and fastest way to load data into Azure Synapse Analytics
- **Deduplication support**: APPEND_DEDUP mode using MERGE statements
- **SSH tunneling**: Supports connecting through SSH tunnels
- **SSL encryption**: Always requires encrypted connections (Azure Synapse requirement)

## Differences from MSSQL Destination

| Feature | MSSQL | Azure Synapse |
|---------|-------|---------------|
| Bulk loading | BULK INSERT + format file | COPY INTO (no format file needed) |
| Authentication | SQL + Windows | SQL + Azure AD/Entra ID |
| SSL | Optional | Always required |
| Data types | Full SQL Server support | Subset (no geometry, geography, xml, etc.) |
| Timestamps | DATETIME | DATETIME2 (recommended) |
| Large strings | VARCHAR(MAX) | NVARCHAR(4000) / VARCHAR(8000) |
| Indexes | Clustered + Nonclustered | Nonclustered only (CCSI default) |
| Connection timeout | 60s | 300s (pool may be paused) |

## Setup

### Prerequisites

1. An Azure Synapse Analytics workspace with a dedicated SQL pool
2. Azure Blob Storage account (for COPY INTO mode)
3. Appropriate permissions (INSERT, ADMINISTER DATABASE BULK OPERATIONS for COPY INTO)

### Connection Parameters

- **Host**: Fully qualified server name (e.g., `yourserver.sql.azuresynapse.net`)
- **Port**: Default `1433`
- **Database**: Dedicated SQL pool name
- **Schema**: Default schema (default: `dbo`)
- **User**: SQL user (format: `user@server_name`)
- **Password**: SQL password

### COPY INTO Configuration

For high-performance loading, configure COPY INTO with:
- Azure Blob Storage account name and container
- Shared Access Signature (SAS) or Account Key for authentication

## Migration from Fivetran

If migrating from Fivetran's Azure Synapse destination:
- Fivetran uses AVRO files for loading; this connector uses CSV with COPY INTO
- Connection parameters are similar (host, port, database, user/password)
- SSH tunnel support is available in both
- Schema mapping and data type handling may differ slightly
