import requests
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine




class BankerMaster:
    def __init__(self):
        """
        Initialize a BankerMaster object.
        """
        self.url_token = 'https://api-parceiro.bancomaster.com.br/token'
        self.payload_token = {"usuario": "44527673000170", "senha": "e~Y'0L8ZGc<8}8fi"}
        self.response_token = requests.post(url=self.url_token, json=self.payload_token)
        self.base_url = "https://api-parceiro.bancomaster.com.br"
        self.token = None

    def cheks_response_status(self):
        """
        Checks the status code of the token request response.
        """
        if self.response_token.status_code == 200:
            self.token = self.response_token.json()["accessToken"]
        else:
            print(f"Error obtaining token. Status code: {self.response_token.status_code}")
            return None

    def auth_headers(self):
        """
        Returns the authentication headers containing the Bearer token.
        """
        if not self.token:
            print("Token not found. Please check the token request.")
            return None
        return {"Authorization": f"Bearer {self.token}", "User-Agent": "ASHER"}


    def consult_health_insurance(self, file_path):
        """
        Makes a request to the health insurance API, formats CPF and phone,
        and saves the response with id_convenio.

        :param file_path: Path to the input CSV or XLSX file.
        """
        if not self.token:
            print("Token is not available. Please check the token request.")
            return

        try:
            # Carrega o arquivo de entrada (CSV ou XLSX)
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, sep=";", dtype="object")
            elif file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path, dtype="object")
            else:
                print("Please provide a valid CSV or XLSX file.")
                return

            # Adiciona a coluna id_convenio e trata campos nulos
            df["id_convenio"] = None
            df["CPF"] = df["CPF"].fillna("").astype(str).str.replace(r"[^\d]", "", regex=True)  # Remove caracteres não numéricos
            df["TELEFONE 1"] = df["TELEFONE 1"].fillna("").astype(str).str.replace(r"[^\d]", "", regex=True)  # Remove caracteres não numéricos

            # Itera sobre as linhas para realizar a consulta
            for index, row in df.iterrows():
                cpf = row["CPF"]
                phone = row["TELEFONE 1"]

                # Valida CPF
                if len(cpf) != 11:
                    print(f"Invalid CPF at index {index}: {cpf}")
                    continue

                # Valida telefone
                if len(phone) < 8:
                    print(f"Invalid phone number at index {index}: {phone}")
                    continue
                
                url = f"{self.base_url}/consignado/v1/cliente/consulta-cpf?cpfRequest={cpf}"
                response = requests.get(url, headers=self.auth_headers())

                if response.status_code == 200:
                    response_json = response.json()
                    if isinstance(response_json, list) and len(response_json) > 0:
                        data = response_json[0]
                        df.at[index, "id_convenio"] = data.get("idConvenio")
                    else:
                        print(f"No valid response data for CPF {cpf}.")
                else:
                    print(f"Failed to fetch `id_convenio` for CPF {cpf}. Status: {response.status_code}")

            output_xlsx = "consult_health_insurance.xlsx"
            df.to_excel(output_xlsx, index=False)
            print(f"File saved as {output_xlsx}.")

        except Exception as e:
            print(f"An error occurred: {str(e)}")


    def process_batch(self, batch_df, batch_number):
        """
        Processes a single batch of rows from the DataFrame.
        """
        api_data = []
        for index, row in batch_df.iterrows():
            cpf = row["CPF"]
            id_convenio = row.get("id_convenio", None)

            if not cpf or len(cpf) != 11:
                print(f"Invalid CPF at index {index}: {cpf}")
                continue
            if not id_convenio:
                print(f"Missing id_convenio for CPF {cpf} at index {index}")
                continue

            url = f"{self.base_url}/consignado/v1/limite/consultar/{cpf}/{id_convenio}"
            response = requests.get(url, headers=self.auth_headers())

            if response.status_code == 200:
                response_json = response.json()
                print(response_json[0]["nome"])
                if isinstance(response_json, list) and response_json:
                    for entry in response_json:
                        base_info = {
                            "cpf": entry.get("cpf"),
                            "nome": entry.get("nome"),
                            "idConvenio": entry.get("idConvenio"),
                            "matricula": entry.get("matricula"),
                            "vlMultiploSaque": entry.get("vlMultiploSaque"),
                            "limiteUtilizado": entry.get("limiteUtilizado"),
                            "limiteTotal": entry.get("limiteTotal"),
                            "limiteDisponivel": entry.get("limiteDisponivel"),
                            "vlLimiteParcela": entry.get("vlLimiteParcela"),
                            "limiteParcelaUtilizado": entry.get("limiteParcelaUtilizado"),
                            "limiteParcelaDisponivel": entry.get("limiteParcelaDisponivel"),
                            "vlMargem": entry.get("vlMargem"),
                            "vlMultiploCompra": entry.get("vlMultiploCompra"),
                            "vlLimiteCompra": entry.get("vlLimiteCompra"),
                            "cdBanco": entry.get("cdBanco"),
                            "cdAgencia": entry.get("cdAgencia"),
                            "cdConta": entry.get("cdConta"),
                            "naoPerturbe": entry.get("naoPerturbe"),
                            "saqueComplementar": entry.get("saqueComplementar"),
                            "refinanciamento": entry.get("contratoRefinanciamento", {}).get("refinanciamento"),
                            "numeroContratos": entry.get("contratoRefinanciamento", {}).get("numeroContratos"),
                            "vlMaximoParcela": entry.get("contratoRefinanciamento", {}).get("vlMaximoParcela"),
                            "vlContrato": entry.get("contratoRefinanciamento", {}).get("valor"),
                        }
                        for opcao in entry.get("opcoes", []):
                            if isinstance(opcao, dict):
                                row_data = base_info.copy()
                                row_data.update(opcao)
                                api_data.append(row_data)
            else:
                print(f"Failed to fetch data for CPF {cpf}. Status: {response.status_code}")

        if api_data:
            api_df = pd.DataFrame(api_data)
            output_csv = f"progress_batch_{batch_number}.csv"
            output_xlsx = f"progress_batch_{batch_number}.xlsx"
            api_df.to_csv(output_csv, sep=";", index=False)
            api_df.to_excel(output_xlsx, index=False)
            print(f"Batch {batch_number} saved: {output_csv}, {output_xlsx}.")
        else:
            print(f"No data found for batch {batch_number}.")

    def extract_consult_health_insurance(self, file_path, max_threads=5):
        if not self.token:
            print("Token is not available. Please check the token request.")
            return

        try:
            # Carrega o arquivo
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, sep=";", dtype="object")
            elif file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path, dtype="object")
            else:
                print("Please provide a valid CSV or XLSX file.")
                return

            df["CPF"] = df["CPF"].str.replace(r"[^\d]", "", regex=True)

            with ThreadPoolExecutor(max_threads) as executor:
                futures = {
                    executor.submit(self.consult_health_insurance, file_path): None
                    for _ in range(max_threads)
                }
                for future in as_completed(futures):
                    future.result()

        except Exception as e:
            print(f"Error processing file: {e}")


    def extract_transform_load_users(self, file_path, credit, max_threads=5):
        """
        Extracts, transforms, and loads user data using threading.
        """
        if not self.token:
            print("Token is not available. Please check the token request.")
            return

        try:
            # Carrega o arquivo
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, sep=";", dtype="object")
            elif file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path, dtype="object")
            else:
                print("Please provide a valid CSV or XLSX file.")
                return

            df["CPF"] = df["CPF"].str.replace(r"[^\d]", "", regex=True)

            total_rows = len(df)
            batches = [df.iloc[i:i + credit] for i in range(0, total_rows, credit)]

            with ThreadPoolExecutor(max_threads) as executor:
                futures = {
                    executor.submit(self.process_batch, batch, i + 1): i + 1
                    for i, batch in enumerate(batches)
                }
                for future in as_completed(futures):
                    batch_number = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Error processing batch {batch_number}: {e}")

        except Exception as e:
            print(f"Error processing file: {e}")

if __name__ == "__main__":
    file_path = "/Users/hedrispereira/Desktop/fast-spreed-sheets/explore_analyze.csv"
    banker_master = BankerMaster()
    banker_master.cheks_response_status()
    banker_master.extract_transform_load_users(file_path, credit=5000, max_threads=10)
