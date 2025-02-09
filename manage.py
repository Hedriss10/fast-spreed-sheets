from src.botmaster import BankerMaster
from src.botmaster import ExtractTransformLoad
from src.controllers.datapaths import ManagePathDatabaseFiles
from rich.console import Console

console = Console()

if __name__ == "__main__":
        
    def interface():
        console.print("Automação Banco Master", style="bold green")
        while True:
            try:
                option = console.input("[bold green]Escolha uma opção: [1]ETL [2]Banker Master [3]Apagar registros [4]Sair.. [/bold green]:")
                if option == "1":
                
                    console.print("Executando ETL...", style="bold green")
                    financialAgreements = input("Deseja processar dataframe sem convenio? (S/N): ")
                    if financialAgreements.upper() == "S":
                        files = ManagePathDatabaseFiles().list_files_database()
                        transformer = ExtractTransformLoad(file_content=files, file_type="csv")
                        transformer.processing_dataframe()
                    else:
                        financialAgreements.upper() == "N"
                        files = ManagePathDatabaseFiles().list_files_database()
                        transformer = ExtractTransformLoad(file_content=files, file_type="csv")
                        transformer.processing_dataframe_financialagreements()
                
                elif option == "2":
                    console.print("Executando Banker Master...", style="bold green")
                    trasnformer = input("[bold green]Deseja buscar convenios? (S/N): [/bold green]")
                    if trasnformer.upper() == "S":
                        banker_master = BankerMaster()
                        banker_master.search_id_convenio()
                    elif transformer.upper() == "N":
                        banker_master = BankerMaster()
                        banker_master.get_limit_users()
                
                elif option == "3":
                    console.print("Apagando registros...", style="bold green")
                    banker_master = BankerMaster()
                    banker_master.trash(financial_agreements=True, generic_report=True, owners_cpf=True, loggers=True)

                elif option == "4":
                    break
                else:
                    console.print("Opcão inválida. Tente novamente.", style="bold red")
            except Exception as e:
                console.print(f"Erro ao executar a opção escolhida: {e}", style="bold red")

    interface()