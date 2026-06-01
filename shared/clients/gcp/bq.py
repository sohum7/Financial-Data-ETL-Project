# BigQuery metadata

# Built-in imports
from dataclasses import dataclass

# Google API imports
from google.cloud import bigquery as bq


@dataclass(slots=True, frozen=True)
class PartitionConfig:
    """Table partitioning configuration data"""
    
    column: str
    granularity: str | None

@dataclass(slots=True, frozen=True)
class TableConfig:
    """Table config data"""
    
    dataset: str
    table: str
    partition_col: PartitionConfig | None
    cluster_cols: list[str] | None # Note ordering matters here
    
    # Get full table name
    @property
    def ds_tbl(self) -> str:
        return f"{self.dataset}.{self.table}"
    
    def __str__(self):
        return f"dataset: {self.dataset}\ntable: {self.table}\npartition column: {self.partition_col}\ncluster columns: {self.cluster_cols}"
    
    def __repr__(self):
        return f"TableConfig(dataset={self.dataset}, table={self.table}, partition_col={self.partition_col}, cluster_cols={self.cluster_cols})"
