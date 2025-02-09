import os
import pandas as pd
import requests
import time

from tqdm import tqdm
from src.utils.log import setup_logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, desc
from src.models.apiresponse import (APIResponse, 
    Base, DATABASE_URL, Loggger, User, UserFinancialAgreements, ReportGeneric
)
from dotenv import load_dotenv
from datetime import datetime

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

load_dotenv()

logger = setup_logger(__name__)

class ExtractTransformLoad:
    
    def __init__(self, file_type: str, file_content: str):
        """
        Initialize an ExtractTransformLoad object.

        Args:
            file_type (str): The type of file. Valid options are 'csv' and 'xlsx'.
            file_content (str): The content of the file to be processed.
        """
        self.file_type = file_type
        self.file_content = file_content
    
    def processing_dataframe(self) :
        """
            Processing dataframe capturation `CPF` not in `convenio_id`
        Args:
            file_content (_type_): str
            file_type (_type_): str
        """
        session = Session()
        try:
            if self.file_type == 'csv':
                df = pd.read_csv(self.file_content, sep=";", dtype="object")
            elif self.file_type == 'xlsx':
                df = pd.read_excel(self.file_content, dtype="object")
            else:
                logger.info("Please provide a valid CSV or XLSX file.")
                return
            
            for index, row in tqdm(df.iterrows(), total=len(df)):
                cpf = row["CPF"].replace(".", "").replace("-", "")
                user = User(cpf=cpf)
                session.add(user)
                session.commit()
            logger.warning("Successfully saved CPFs to the database.")
        except Exception as e:
            logger.error(f"Error processing file: {e}", exc_info=True)
        finally:
            session.close()

    def processing_dataframe_financialagreements(self):
        """
            Processing dataframe capturation `CPF` with `convenio_id`
        Args:
            file_content (_type_): str
            file_type (_type_): str
        """
        session = Session()
        try:
            if self.file_type == 'csv':
                df = pd.read_csv(self.file_content, sep=";", dtype="object")
            elif self.file_type == 'xlsx':
                df = pd.read_excel(self.file_content, dtype="object")
            else:
                logger.info("Please provide a valid CSV or XLSX file.")
                return
            
            for index, row in tqdm(df.iterrows(), total=len(df)):
                cpf = row["CPF"]
                id_convenio = row["id_convenio"]

                user = UserFinancialAgreements(cpf=cpf, id_convenio=id_convenio)
                session.add(user)
                session.commit()

            logger.warning("Successfully saved CPFs with id_convenio to the database.")
        except Exception as e:
            logger.error(f"Error processing file: {e}", exc_info=True)
        finally:
            session.close()

class UserAgentsRequests:

    def __init__(self):
        self.request_count = 0
        self.last_reset_time = time.time()

    def agente_request(self, url: str, headers: dict):
        """
        Check the status code of the token request response.

        :param url: URL da requisição.
        :param payload: Dados a serem enviados na requisição.
        :param headers: Cabeçalhos da requisição.
        :param method: Método HTTP (GET, POST, etc.).
        """
        try:
            current_time = datetime.now().time()
            if current_time >= datetime.strptime('07:00', '%H:%M').time() and current_time <= datetime.strptime('20:00', '%H:%M').time():
                max_requests_per_minute = 100
            else:
                max_requests_per_minute = 2000

            if self.request_count >= max_requests_per_minute:
                elapsed_time = time.time() - self.last_reset_time
                if elapsed_time < 60:
                    sleep_time = 60
                    logger.info(f"Rate limit reached. Sleeping for {sleep_time} seconds.")
                    time.sleep(sleep_time)
                self.request_count = 0
                self.last_reset_time = time.time()

            response = requests.get(url, headers=headers)
            self.request_count += 1

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                return {'message': 'Unauthorized'}
            else:
                logger.error(f"Failed to fetch data. Status: {response.status_code}")
                return response.json()

        except Exception as e:
            logger.error(f"Error processing file: {e}", exc_info=True)
            return None

class BankerMaster:
    def __init__(self):
        """
        Initialize a BankerMaster object.
        """
        self.url_token = os.getenv("URL_TOKEN")
        self.payload_token = {"usuario": os.getenv("USERMASTER"), "senha": os.getenv("MASTERPASSWORD")}
        self.base_url = os.getenv("BASE_URL")
        self.token = None
    
    def refresh_token(self):
        """
        Checks the status code of the token request response.
        """
        session = Session()
        try:
            response = requests.post(url=self.url_token, json=self.payload_token)
            if response.status_code == 200:
                response.json()["accessToken"]
                print(response.text)
                token = Loggger(message=f"{response.json()['accessToken']}", exception=None)
                session.add(token)
                session.commit()
            else:
                logger.error(f"Failed to fetch token. Status: {self.response_token.status_code}")
                session.add(Loggger(exception=f"{logger.error(f'Failed to fetch token. Status: {self.response_token.status_code}')}"))
                session.commit()
        except Exception as e:
            session.add(Loggger(exception=f"{logger.error(f'Error processing file: {e}')}"))
        finally:
            session.close()
    
    def auth_headers(self):
        """
        Returns the authentication headers containing the Bearer token.
        If the token is not available in memory, it fetches the last saved token from the database.
        """
        if not self.token:
            session = Session()
            try:
                last_log = session.query(Loggger).order_by(desc(Loggger.created_at)).first()
                
                if last_log:
                    self.token = last_log.message
                    logger.info("Token sucessfully recovered from the database.")
                else:
                    logger.warning("Token not found in the database.")
                    return None
            except Exception as e:
                session.add(Loggger(exception=f"{logger.error(f'Error processing file: {e}')}"))
                return None
            finally:
                session.close()
                
        return {"Authorization": f"Bearer {self.token}", "User-Agent": "ASHER"}

    def search_id_convenio(self):
        session = Session()
        agents_requests = UserAgentsRequests()
        
        try:
            cpfs = session.query(User.cpf).all()
            for cpf in tqdm(cpfs, desc="Processing CPFs"):
                self.cpf = cpf[0]
                url = f"{self.base_url}/consignado/v1/cliente/consulta-cpf?cpfRequest=47795867120"

                max_retries = 2
                attempts = 0  

                while attempts < max_retries:
                    response = agents_requests.agente_request(url=url, headers=self.auth_headers())

                    if response is not None and isinstance(response, list):
                        for item in response:
                            id_convenio = item.get("idConvenio")                    
                            if id_convenio:
                                user = UserFinancialAgreements(cpf=self.cpf, id_convenio=id_convenio)
                                session.add(user)
                                session.commit()
                                tqdm.write(f"Successfully saved CPF {self.cpf} with id_convenio {id_convenio} to the database.")
                            else:
                                tqdm.write(f"idConvenio not found in response for CPF {self.cpf}.")
                                session.add(ReportGeneric(cpf=self.cpf, message=f"{response}", id_convenio=None))
                                session.commit()
                        break
                    
                    elif isinstance(response, dict) and response.get("message") == "Unauthorized":
                        tqdm.write(f"Unauthorized for CPF {self.cpf}. Refreshing token...")
                        self.trash(generic_report=False, financial_agreements=False, owners_cpf=False, loggers=True)
                        self.refresh_token()
                        attempts += 1

                    else:
                        tqdm.write(f"Failed to fetch data for CPF {self.cpf}.")
                        session.add(ReportGeneric(cpf=self.cpf, message=f"{response}", id_convenio=None))
                        session.commit()
                        break 
        except Exception as e:
            tqdm.write(f"Error processing file: {e}")
        finally:
            session.close()

    def get_limit_users(self):
        """
        Processing each line of the database and saving the user limits to the database.
        """
        session = Session()
        agents_requests = UserAgentsRequests()

        try:
            owners = session.query(UserFinancialAgreements).all()
            
            for owner in tqdm(owners, desc="Consult limit for cpf"):
                cpf = owner.cpf
                id_convenio = owner.id_convenio
                url = f"{self.base_url}/consignado/v1/limite/consultar/{cpf}/{id_convenio}"                
                
                max_retries = 2  # Número máximo de reprocessamentos
                attempts = 0  

                while attempts < max_retries:
                    response = agents_requests.agente_request(url=url, headers=self.auth_headers())

                    if response is not None and isinstance(response, list):
                        for item in response:
                            limit = APIResponse(
                                cpf=item.get("cpf"),
                                nome=item.get("nome"),
                                id_convenio=item.get("idConvenio"),
                                matricula=item.get("matricula"),
                                vl_multiplo_saque=item.get("vlMultiploSaque"),
                                limite_utilizado=item.get("limiteUtilizado"),
                                limite_total=item.get("limiteTotal"),
                                limite_disponivel=item.get("limiteDisponivel"),
                                vl_limite_parcela=item.get("vlLimiteParcela"),
                                limite_parcela_utilizado=item.get("limiteParcelaUtilizado"),
                                limite_parcela_disponivel=item.get("limiteParcelaDisponivel"),
                                vl_margem=item.get("vlMargem"),
                                vl_multiplo_compra=item.get("vlMultiploCompra"),
                                vl_limite_compra=item.get("vlLimiteCompra"),
                                cd_banco=item.get("cdBanco"),
                                cd_agencia=item.get("cdAgencia"),
                                cd_conta=item.get("cdConta"),
                                nao_perturbe=item.get("naoPerturbe"),
                                saque_complementar=item.get("saqueComplementar"),
                                refinanciamento=item.get("contratoRefinanciamento", {}).get("refinanciamento"),
                                numero_contrato=item.get("contratoRefinanciamento", {}).get("numeroContratos"),
                                vlMaximoParcelas=item.get("contratoRefinanciamento", {}).get("vlMaximoParcela"),
                                vlContrato=item.get("contratoRefinanciamento", {}).get("valor")
                            )
                            session.add(limit)
                            session.commit()
                            tqdm.write(f"Successfully saved limit for CPF {cpf}.")
                        break  # Sai do loop se a requisição for bem-sucedida
                    
                    elif isinstance(response, dict) and response.get("message") == "Unauthorized":
                        tqdm.write(f"Unauthorized for CPF {cpf}. Refreshing token...")
                        self.trash(generic_report=False, financial_agreements=False, owners_cpf=False, loggers=True)
                        self.refresh_token()  # Renova o token
                        attempts += 1  # Incrementa a tentativa e reprocessa

                    else:
                        tqdm.write(f"Failed to fetch data for CPF {cpf}.")
                        session.add(ReportGeneric(cpf=cpf, message=f"{response}", id_convenio=id_convenio))
                        session.commit()
                        break  # Sai do loop se não for erro de autenticação
        except Exception as e:
            tqdm.write(f"Error processing file: {e}")
        finally:
            session.close()

    def trash(self, generic_report: bool, financial_agreements: bool, owners_cpf: bool, loggers: bool):
        session = Session()        
        try:
            if generic_report:
                session.query(ReportGeneric).delete()
                session.commit()
                logger.info("Report deleted successfully.")
            
            elif financial_agreements:
                session.query(UserFinancialAgreements).delete()
                session.commit()
                logger.info("User FinancialAgreements deleted successfully.")
            
            elif owners_cpf:
                session.query(User).delete()
                session.commit()
                logger.info("User deleted successfully.")
                
            elif loggers:
                session.query(Loggger).delete()
                session.commit()
                logger.info("Loggers deleted successfully.")
            
        except Exception as e:
            logger.error(f"Error processing file: {e}", exc_info=True)
        finally:
            session.close()