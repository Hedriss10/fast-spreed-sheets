from typing import List, Literal
from pandas import read_csv, read_excel, DataFrame
from os import path


class SpreadSheets:
    def __init__(self, file_path: str, file_type: Literal["csv", "xlsx"], sheet_name: str = None, columns: List[str] = None) -> None:
        self.file_path = file_path
        self.data = None
        self.file_type = file_type
        self.sheet_name = sheet_name
        self.columns = columns

    def read(self) -> DataFrame:
        """
        Reads data from the specified file path based on the file type.
        
        Returns:
            DataFrame: The data read from the file.
        """
        if self.file_type == "csv":
            self.data = read_csv(self.file_path, usecols=self.columns, dtype=str, sep=";")
        elif self.file_type == "xlsx":
            self.data = read_excel(self.file_path, sheet_name=self.sheet_name, usecols=self.columns, dtype=str)
        else:
            raise Exception("Invalid file type")
        return self.data

    def write(self, data: DataFrame) -> None:
        """
        Writes data to the specified file path based on the file type.
        
        Args:
            data (DataFrame): The data to write to the file.
        """
        if self.file_type == "csv":
            data.to_csv(self.file_path, index=False, sep=";")
        elif self.file_type == "xlsx":
            data.to_excel(self.file_path, sheet_name=self.sheet_name, index=False)
        else:
            raise Exception("Invalid file type")

    def load_data(self) -> DataFrame:
        """
        Loads data into memory by reading the file.
        
        Returns:
            DataFrame: The loaded data.
        """
        self.data = self.read()
        return self.data

    def handle_missing_data(self, drop: bool = True) -> DataFrame:
        """
        Checks for missing data and removes rows if specified.
        
        Args:
            drop (bool): If True, removes rows with missing values in the specified columns.
        
        Returns:
            DataFrame: Processed DataFrame (with or without missing data removed).
        """
        try:
            df = self.load_data()
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
        Saves the given DataFrame back to the file.
        
        Args:
            data (DataFrame): The data to save.
        """
        self.write(data)


if __name__ == "__main__":
    file_path = "data.csv"
    columns = ["CPF", "Name", "Phone"]

    spreadsheet = SpreadSheets(file_path, file_type="csv", columns=columns)

    processed_data = spreadsheet.handle_missing_data(drop=True)

    print(processed_data)
