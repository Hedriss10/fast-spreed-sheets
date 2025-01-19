import os
import shutil

class ManagePathDatabaseFiles:
    def __init__(self):
        self.data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        self.trash_path = os.path.join(self.data_path, "trash")
        self.output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")

        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(self.trash_path, exist_ok=True)

    def save_path_data(self):
        """
        Save the paths to the data and trash directories.
        """
        return self.data_path

    def list_files_database(self):
        """
        List all .xlsx and .csv files in the "data" directory.
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
