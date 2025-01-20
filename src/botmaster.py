import io
import os
import pandas as pd
import requests
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models.apiresponse import APIResponse, Base, DATABASE_URL
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


load_dotenv()

class BankerMaster:
    def __init__(self, phone_columns):
        """
        Initialize a BankerMaster object.
        """
        self.url_token = os.getenv("URL_TOKEN")
        self.payload_token = {"usuario": os.getenv("USER"), "senha": os.getenv("PASSWORD")}
        self.response_token = requests.post(url=self.url_token, json=self.payload_token)
        self.base_url = os.getenv("BASE_URL")
        self.token = None
        self.phone_columns = phone_columns

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

    def process_batch(self, batch_df, batch_number):
        """
        Processes a single batch of rows from the DataFrame and saves to the database.
        """

        session = Session()
        try:
            for index, row in batch_df.iterrows():
                cpf = row["CPF"]
                id_convenio = row.get("id_convenio", None)
                phone = row[f'{self.phone_columns}']
                
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
                    if isinstance(response_json, list) and response_json:
                        for entry in response_json:
                            # Prepare the data to save
                            api_response = APIResponse(
                                cpf=entry.get("cpf"),
                                nome=entry.get("nome"),
                                phone=phone,
                                id_convenio=entry.get("idConvenio"),
                                matricula=entry.get("matricula"),
                                vl_multiplo_saque=entry.get("vlMultiploSaque"),
                                limite_utilizado=entry.get("limiteUtilizado"),
                                limite_total=entry.get("limiteTotal"),
                                limite_disponivel=entry.get("limiteDisponivel"),
                                vl_limite_parcela=entry.get("vlLimiteParcela"),
                                limite_parcela_utilizado=entry.get("limiteParcelaUtilizado"),
                                limite_parcela_disponivel=entry.get("limiteParcelaDisponivel"),
                                vl_margem=entry.get("vlMargem"),
                                vl_multiplo_compra=entry.get("vlMultiploCompra"),
                                vl_limite_compra=entry.get("vlLimiteCompra"),
                                cd_banco=entry.get("cdBanco"),
                                cd_agencia=entry.get("cdAgencia"),
                                cd_conta=entry.get("cdConta"),
                                nao_perturbe=entry.get("naoPerturbe"),
                                saque_complementar=entry.get("saqueComplementar"),
                                refinanciamento=entry.get("contratoRefinanciamento", {}).get("refinanciamento"),
                                numero_contrato=entry.get("contratoRefinanciamento", {}).get("numeroContratos"),
                                vlMaximoParcelas=entry.get("contratoRefinanciamento", {}).get("vlMaximoParcela"),
                                vlContrato=entry.get("contratoRefinanciamento", {}).get("valor")
                            )
                            # Add to session and commit
                            session.add(api_response)
                            try:
                                session.commit()
                            except IntegrityError as e:
                                print(f"Error inserting data for CPF {cpf}: {e}")
                                session.rollback()
                else:
                    print(f"Failed to fetch data for CPF {cpf}. Status: {response.status_code}")
            print(f"Batch {batch_number} processed successfully.")
        except Exception as e:
            session.rollback()
            print(f"Error processing batch {batch_number}: {e}")
        finally:
            session.close()

    def extract_transform_load_users(self, file_type, file_content, credit, max_threads=5):
        """
        Extracts, transforms, and loads user data using threading and saves directly to the database.
        """
        if not self.token:
            print("Token is not available. Please check the token request.")
            return

        try:
            # Load file
            if file_type == 'csv':
                df = pd.read_csv(io.StringIO(file_content), sep=",", dtype="object")
            elif file_type == 'xlsx':
                df = pd.read_excel(io.BytesIO(file_content), dtype="object")
            else:
                print("Por favor, forneça um arquivo CSV ou XLSX válido.")
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

    def simulator_values(self):
        ## todo falta implementar
        # mock payload
        paylaod = {
            "cpf": "164.247.602-10",
            "idConvenio": "184",
            "matricula": "0847163"
        }
        url = f"{self.base_url}/consignado/v1/simulacao"
        print("url", url)
        response = requests.post(url, headers=self.auth_headers(), json=paylaod)
        print(response.json())
        # return response

    def simulator_values_batch(self,): 
        # todo falta implementar
        ...

    def extract_data_database(self, filter_column=None, filter_operator=None, filter_value=None):
        try:
            session = Session()
            data = session.query(APIResponse).all()
            data_dict = [record.__dict__ for record in data]
            
            for record in data_dict:
                record.pop('_sa_instance_state', None)

            df = pd.DataFrame(data_dict)

            if filter_column and filter_operator and filter_value is not None:
                if filter_operator in ['>', '<', '>=', '<=', '==', '!=']:
                    df = df.query(f"{filter_column} {filter_operator} @filter_value")
                else:
                    print(f"Operador inválido: {filter_operator}")

            print(df.head())
            
            session.close()
            return df

        except Exception as e:
            print(f"Error processing file: {e}")

if __name__ == "__main__":
    file_path = "/Users/hedrispereira/Desktop/fast-spreed-sheets/explore_analyze.csv"
    banker_master = BankerMaster(phone_columns="TELEFONE")
    banker_master.cheks_response_status()
    # banker_master.extract_transform_load_users(file_path, credit=5000, max_threads=10)
    banker_master.extract_data_database()