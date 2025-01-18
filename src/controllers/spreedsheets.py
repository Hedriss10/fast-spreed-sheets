# src/controllers/spreedsheets.py
import os
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

    def search_columns_data(self, search_columns: str, questions_columns: list, df2: pd.DataFrame) -> pd.DataFrame:
        """
        Search for specific columns in two DataFrames based on a common column (e.g., phone numbers)
        and returns the intersection, including only the specified columns in `questions_columns`.

        Args:
            search_columns (str): The column name to use for intersection (e.g., phone number).
            questions_columns (list): List of columns to include in the result.
            df1 (pd.DataFrame): The first DataFrame.
        Returns:
            pd.DataFrame: A DataFrame containing the intersection and specified columns.
        """
        try:
            if not isinstance(questions_columns, list):
                raise ValueError("questions_columns must be a list of column names.")

            intersection = self.load_data().merge(df2, how="inner", on=search_columns)
            result = intersection[questions_columns]
            return result, intersection

        except Exception as e:
            raise Exception(f"Error while searching for columns: {e}")

    def remove_lines_data(
        self,
        chunk_size: int,
        data: pd.DataFrame,
        output_path: str,
        file_prefix: str = "chunk",
        encoding: str = "utf-8",
        sep: str = ";",
        index: bool = False,
        file_type: str = "csv"
    ) -> list:
        """
        Splits a DataFrame into smaller chunks with a specified number of rows and saves them as separate files.

        Args:
            chunk_size (int): The number of rows per chunk.
            data (pd.DataFrame): The input DataFrame.
            output_path (str): Directory where the chunks will be saved.
            file_prefix (str): Prefix for the output file names (default is "chunk").
            encoding (str): Encoding for the output files (default is "utf-8").
            sep (str): Separator for the output files (default is ";").
            index (bool): Whether to include the index in the output files (default is False).
            file_type (str): Type of the output files ("csv" or "txt", default is "csv").

        Returns:
            list: A list of file paths where the chunks were saved.
        """
        try:
            if data.empty:
                raise ValueError("The input DataFrame is empty.")

            os.makedirs(output_path, exist_ok=True)

            file_paths = []

            for i in range(0, len(data), chunk_size):
                chunk = data.iloc[i:i + chunk_size]
                file_name = f"{file_prefix}_{i // chunk_size + 1}.{file_type}"
                file_path = f"{output_path}/{file_name}"

                if file_type == "csv":
                    chunk.to_csv(file_path, index=index, sep=sep, encoding=encoding)
                elif file_type == "txt":
                    chunk.to_csv(file_path, index=index, sep=sep, encoding=encoding, header=False)
                else:
                    raise ValueError(f"Unsupported file type: {file_type}")

                file_paths.append(file_path)

            return file_paths

        except Exception as e:
            raise Exception(f"Error while splitting and saving DataFrame into chunks: {e}")

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

            search_columns = "phone"
            questions_columns = ["phone", "name", "email", "address", "city"]

            spreadsheet = SpreadSheets(file_path=file_path, file_type=file_type, columns=columns)

            df = spreadsheet.load_data()
            # generic_reports = spreadsheet.generic_reports()
            # handle_missing_data = spreadsheet.handle_missing_data(drop=True)
            separate_chunks = spreadsheet.remove_lines_data(chunk_size=1000, data=df, output_path="output", file_prefix="chunk", encoding="utf-8", sep=";", index=False, file_type="csv")
            # serch_columns = spreadsheet.search_columns_data(df1=df1, df2=df2, search_columns=search_columns, questions_columns=questions_columns)
            
        except Exception as e:
            print(f"Erro ao processar o arquivo {file_path}: {e}")
