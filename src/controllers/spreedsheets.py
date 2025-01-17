import pandas as pd
import chardet
from os import path
from typing import List, Literal
from datapaths import ManagePathDatabaseFiles


class SpreadSheets:
    def __init__(
        self, file_path: str, file_type: Literal["csv", "xlsx"], sheet_name: str = None, columns: List[str] = None
    ) -> None:
        self.file_path = file_path
        self.file_type = file_type
        self.sheet_name = sheet_name
        self.columns = columns or []
        self.data = None

    def detect_encoding(self) -> str:
        """
        Detects the file encoding using chardet.
        Provides a fallback to 'latin1' if detection fails.
        """
        try:
            with open(self.file_path, "rb") as file:
                result = chardet.detect(file.read())
                encoding = result.get("encoding") or "latin1"
                return encoding
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {self.file_path}. Error: {e}")
        except Exception as e:
            print(f"Warning: Unable to detect encoding, defaulting to 'latin1'. Error: {e}")
            return "latin1"

    def read_file(self) -> pd.DataFrame:
        """
        Reads the file based on its type and returns it as a DataFrame.
        """
        try:
            encoding = self.detect_encoding()

            if self.file_type == "csv":
                data = pd.read_csv(self.file_path, usecols=self.columns or None, dtype=str, sep=";", encoding=encoding)

            elif self.file_type == "xlsx":
                data = pd.read_excel(self.file_path, sheet_name=None, usecols=self.columns or None, dtype=str)

                if self.sheet_name is None:
                    self.sheet_name = list(data.keys())[0]
                
                data = data.get(self.sheet_name, None)
                if data is None:
                    raise ValueError(f"Sheet '{self.sheet_name}' not found in the Excel file.")

            else:
                raise ValueError(f"Unsupported file type: {self.file_type}")
            
            if not isinstance(data, pd.DataFrame):
                raise TypeError(f"Expected a DataFrame, but got {type(data)} instead.")
            
            return data

        except Exception as e:
            raise Exception(f"Error while reading the file: {e}")

    def load_data(self) -> pd.DataFrame:
        """
        Loads data and stores it in self.data.
        Ensures that data is always a DataFrame.
        """
        if self.data is None:
            self.data = self.read_file()
        
        if not isinstance(self.data, pd.DataFrame):
            raise ValueError(f"Expected a DataFrame, but got {type(self.data)} instead.")

        return self.data

    def generic_reports(self) -> dict:
        """
        Generates a report of missing data counts for the DataFrame.
        """
        try:
            df = self.load_data()
            count_lines_missing = df.isnull().sum()
            dict = count_lines_missing.to_dict()
            return dict
        except Exception as e:
            raise Exception(f"Error generating generic report: {e}")

    def handle_missing_data(self, drop: bool = True) -> pd.DataFrame:
        """
        Handles missing data in the DataFrame, optionally dropping rows with missing values.
        """
        try:
            df = self.load_data()

            if self.columns:
                missing_columns = [col for col in self.columns if col not in df.columns]
                if missing_columns:
                    raise ValueError(f"Missing columns in the file: {missing_columns}")

            if drop:
                df = df.dropna(subset=self.columns)
                
            print("Data after handling missing values:")
            self.save_data(df)
            return df
        except Exception as e:
            raise Exception(f"Error while handling missing data: {e}")

    def save_data(self, data: pd.DataFrame) -> None:
        """
        Saves the DataFrame back to the file.
        """
        try:
            if self.file_type == "csv":
                data.to_csv(self.file_path, index=False, sep=";")
            elif self.file_type == "xlsx":
                data.to_excel(self.file_path, sheet_name=self.sheet_name, index=False)
            else:
                raise ValueError(f"Unsupported file type: {self.file_type}")
            print("Data saved successfully.")
        except Exception as e:
            raise Exception(f"Error while saving the file: {e}")


if __name__ == "__main__":
    file_paths = ManagePathDatabaseFiles().list_files_database()

    if not file_paths or not isinstance(file_paths, list):
        raise ValueError("Nenhum arquivo encontrado ou caminhos inválidos.")

    for file_path in file_paths:
        try:
            print(f"Processando o arquivo: {file_path}")
            
            file_type = "xlsx" if file_path.endswith(".xlsx") else "csv"
            columns = ["CPF", "id_convenio"]

            spreadsheet = SpreadSheets(file_path=file_path, file_type=file_type, columns=columns)

            df = spreadsheet.load_data()
            generic_reports = spreadsheet.generic_reports()
            handle_missing_data = spreadsheet.handle_missing_data(drop=True)
        except Exception as e:
            print(f"Erro ao processar o arquivo {file_path}: {e}")
