from src.botmaster import BankerMaster
from src.botmaster import ExtractTransformLoad

if __name__ == "__main__":
    
    def main():
        transformer = ExtractTransformLoad(file_content="", file_type="xlsx")
        transformer.processing_dataframe() 
        transformer.processing_dataframe_financialagreements()
        
    def consult():
        banker_master = BankerMaster()
        banker_master.cheks_response_status()
        banker_master.auth_headers()
        banker_master.search_id_convenio()
        banker_master.get_limit_users()
        banker_master.trash(financial_agreements=False, generic_report=False, owners_cpf=False, loogers=True)