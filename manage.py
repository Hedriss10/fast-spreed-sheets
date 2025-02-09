from src.botmaster import BankerMaster
from src.botmaster import ExtractTransformLoad
from src.controllers.datapaths import ManagePathDatabaseFiles

if __name__ == "__main__":
    
    def etl():
        files = ManagePathDatabaseFiles().list_files_database()
        transformer = ExtractTransformLoad(file_content=files, file_type="csv")
        transformer.processing_dataframe()
        transformer.processing_dataframe_financialagreements()
        
    def requestsBancoMaster():
        banker_master = BankerMaster()
        banker_master.cheks_response_status()
        banker_master.auth_headers()
        banker_master.search_id_convenio()
        banker_master.get_limit_users()
        banker_master.trash(financial_agreements=False, generic_report=False, owners_cpf=False, loggers=False)
    
    def interface():
        ...
    