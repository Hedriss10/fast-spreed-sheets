# src/models/etl.py
from models import (TablesProcessing, 
                    ModelsProcessing, 
                    DataProcessing)

class ETL:
    def __init__(self, tables_processing: TablesProcessing, models_processing: ModelsProcessing, data_processing: DataProcessing) -> None:
        self.tables_processing = tables_processing
        self.models_processing = models_processing
        self.data_processing = data_processing
    
    def __str__(self):
        return f"ETL(tables_processing={self.tables_processing!r}, models_processing={self.models_processing!r})"
    
    def __repr__(self):
        return f"ETL(tables_processing={self.tables_processing!r}, models_processing={self.models_processing!r})"

    
    def __eq__(self, other):
        if not isinstance(other, ETL):
            return False
        return self.tables_processing == other.tables_processing and self.models_processing == other.models_processing

    def get_all_tables(self, data_processing: DataProcessing) -> list:
        ...
        
    def transform_tables(self):
        ...
        
    def export_tables(self):
        ...