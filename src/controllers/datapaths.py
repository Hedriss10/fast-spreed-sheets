import os
import shutil

class ManagePathDatabaseFiles:
    def __init__(self):
        self.data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        self.trash_path = os.path.join(self.data_path, "trash")

        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(self.trash_path, exist_ok=True)

    def list_files_database(self):
        """
        Lista os arquivos disponíveis no diretório "data" que têm extensão .xlsx ou .csv.
        """
        try:
            files = [
                file for file in os.listdir(self.data_path)
                if file.endswith(".xlsx") or file.endswith(".csv")
            ]

            if not files:
                print("Nenhum arquivo .xlsx ou .csv encontrado na pasta 'data'.")
                return []
            
            return files
        except Exception as e:
            print(f"Erro ao listar arquivos no banco de dados: {e}")
            return []

    def move_trash_files(self, move_trash: bool = False) -> None:
        """
        Move os arquivos .xlsx e .csv do diretório "data" para a pasta "trash" se move_trash for True.
        """
        try:
            if not move_trash:
                print("Ação de mover arquivos não foi ativada.")
                return

            files = [
                file for file in os.listdir(self.data_path)
                if file.endswith(".xlsx") or file.endswith(".csv")
            ]

            if not files:
                print("Nenhum arquivo para mover para a lixeira.")
                return

            for file in files:
                source_path = os.path.join(self.data_path, file)
                destination_path = os.path.join(self.trash_path, file)
                shutil.move(source_path, destination_path)
                os.remove(destination_path)
                print(f"Arquivo movido para a lixeira: {file}")
        except Exception as e:
            print(f"Erro ao mover arquivos para a lixeira: {e}")

if __name__ == "__main__":
    list_path_files = ManagePathDatabaseFiles()
    list_path_files.list_files_database()
    list_path_files.move_trash_files(move_trash=True)