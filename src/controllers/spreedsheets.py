from typing import List, Literal
from pandas import read_csv, read_excel, read_json, DataFrame
from os import path
import chardet


class SpreadSheets:
    def __init__(
        self, file_path: path, file_type: Literal["csv", "xlsx", "json"], sheet_name: str = None, columns: List[str] = None
    ) -> None:
        self.file_path = file_path
        self.file_type = file_type
        self.sheet_name = sheet_name
        self.columns = columns or []
        self.data = None

    def detect_encoding(self) -> str:
        """
        Detects the encoding of the file to avoid decoding issues.
        
        Returns:
            str: The detected encoding.
        """
        with open(self.file_path, "rb") as file:
            result = chardet.detect(file.read())
            return result.get("encoding", "utf-8")

    def read_file(self) -> DataFrame:
        """
        Reads a file based on its type and handles encoding and format issues.
        
        Returns:
            DataFrame: The loaded data.
        """
        try:
            encoding = self.detect_encoding()
            if self.file_type == "csv":
                return read_csv(self.file_path, usecols=self.columns, dtype=str, sep=";", encoding=encoding)
            elif self.file_type == "xlsx":
                return read_excel(self.file_path, sheet_name=self.sheet_name, usecols=self.columns, dtype=str)
            elif self.file_type == "json":
                return read_json(self.file_path, encoding=encoding)
            else:
                raise ValueError(f"Unsupported file type: {self.file_type}")
        except Exception as e:
            raise Exception(f"Error while reading the file: {e}")

    def load_data(self) -> DataFrame:
        """
        Loads data from the specified file path and stores it in self.data.
        
        Returns:
            DataFrame: The loaded data.
        """
        self.data = self.read_file()
        return self.data

    def handle_missing_data(self, drop: bool = True) -> DataFrame:
        """
        Checks for missing data and optionally removes rows with missing values.
        
        Args:
            drop (bool): If True, removes rows with missing values in the specified columns.
        
        Returns:
            DataFrame: Processed DataFrame.
        """
        try:
            df = self.load_data()
            if not isinstance(df, DataFrame):
                raise TypeError("The loaded data is not a DataFrame.")
            
            if self.columns:
                missing_columns = [col for col in self.columns if col not in df.columns]
                if missing_columns:
                    raise ValueError(f"The columns {missing_columns} were not found in the file.")

            if drop:
                df = df.dropna(subset=self.columns)
                self.save_data(df)
            return df
        except Exception as e:
            raise Exception(f"Error while handling missing data: {e}")

    def save_data(self, data: DataFrame) -> None:
        """
        Saves the processed data back to the file.
        
        Args:
            data (DataFrame): The DataFrame to save.
        """
        try:
            if self.file_type == "csv":
                data.to_csv(self.file_path, index=False, sep=";")
            elif self.file_type == "xlsx":
                data.to_excel(self.file_path, sheet_name=self.sheet_name, index=False)
            elif self.file_type == "json":
                data.to_json(self.file_path, orient="records")
            else:
                raise ValueError(f"Unsupported file type: {self.file_type}")
        except Exception as e:
            raise Exception(f"Error while saving the file: {e}")


# Example usage
if __name__ == "__main__":
    file_path = "/Users/hedrispereira/Desktop/fast-spreed-sheets/database/generic_report.xlsx"
    columns = ["CPF", "Name", "Phone"]

    spreadsheet = SpreadSheets(file_path, file_type="csv", columns=columns)
    try:
        processed_data = spreadsheet.handle_missing_data(drop=False)
        print(processed_data)
    except Exception as e:
        print(f"An error occurred: {e}")
