# data/dataload.py
import os 

class ManagePathDatabaseFiles:
    
    def list_files_database(self):
        try:
            file_path = os.path.dirname(os.path.abspath(__file__))
            for file in os.listdir(file_path):
                if file.endswith(".xlsx") or file.endswith(".csv"):
                    return file
        except Exception as e:
            print(f"Error execute list files database {e}")

    def move_trash_files(self, move_trash: bool = False) -> None:
        try:
            file_path = os.path.dirname(os.path.abspath(__file__))
            
            if move_trash:
                for file in os.listdir(file_path):
                    if file.endswith(".xlsx") or file.endswith(".csv"):
                        file_path = os.path.join(file_path, file)
                        os.remove(file_path)
            else:
                print("No files to move.")

        except Exception as e:
            print(f"Error execute move trash files {e}")
        
    
if __name__ == "__main__":
    list_path_files = ManagePathDatabaseFiles()
    list_path_files.move_trash_files(move_trash=False)