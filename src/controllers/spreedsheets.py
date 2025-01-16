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
        if self.file_type == "csv":
            self.data = read_csv(self.file_path, usecols=self.columns, dtype=str, sep=";")
        elif self.file_type == "xlsx":
            self.data = read_excel(self.file_path, sheet_name=self.sheet_name, usecols=self.columns, dtype=str)
        else:
            raise Exception("Invalid file type")
        return self.data

    def write(self, data: DataFrame) -> None:
        if self.file_type == "csv":
            data.to_csv(self.file_path, index=False, sep=";")
        elif self.file_type == "xlsx":
            data.to_excel(self.file_path, sheet_name=self.sheet_name, index=False)
        else:
            raise Exception("Invalid file type")

    def load_data(self) -> DataFrame:
        self.data = self.read()
        return self.data

    def absent(self, drop: bool = True) -> DataFrame:
        """
            Verifica dados ausentes e os remove, se necessário.
        Args:
            drop (bool): Se True, remove linhas com dados ausentes nas colunas especificadas.
        Returns:
            DataFrame: DataFrame processado (com ou sem remoção de dados ausentes).
        """
        try:
            df = self.load_data()
            if self.columns:
                missing_columns = [col for col in self.columns if col not in df.columns]
                if missing_columns:
                    raise ValueError(f"As colunas {missing_columns} não foram encontradas no arquivo.")
            
            if drop:
                df = df.dropna(subset=self.columns)
                self.save_data(df)
            return df
        except Exception as e:
            raise Exception(f"Erro ao processar dados ausentes: {e}")

    def save_data(self, data: DataFrame) -> None:
        self.write(data)

if __name__ == "__main__":
    file_path = "dados.csv"
    columns = ["CPF", "Nome", "Telefone"]

    spreadsheet = SpreadSheets(file_path, file_type="csv", columns=columns)

    processed_data = spreadsheet.absent(drop=True)

    print(processed_data)
