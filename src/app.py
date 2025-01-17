# src/app.py
import streamlit as st
import requests
import json
import pandas as pd
import os


def add_footer():
    """
    Add a footer to the Streamlit sidebar with the text "maisbs".
    
    This is a simple function that adds a footer to the Streamlit sidebar with the text "maisbs".
    It is intended to be used to add a footer to the sidebar of a Streamlit app.
    """
    st.sidebar.markdown("""
        <div style="text-align: center; padding: 10px;">
            maisbs
        </div>
    """, unsafe_allow_html=True)


class BankerMaster:

    def __init__(self):
        """
        Initialize a BankerMaster object.
        
        This method initializes a BankerMaster object.
        It sets the URL and payload for the token request and sends the request.
        It also sets the base URL for the API.
        """
        self.url_token = 'https://api-parceiro.bancomaster.com.br/token'
        self.payload_token = {"usuario": "44527673000170", "senha": "e~Y'0L8ZGc<8}8fi"}
        self.response_token = requests.post(url=self.url_token, json=self.payload_token)
        self.base_url = f"""https://api-parceiro.bancomaster.com.br"""

    def cheks_response_status(self):
        """
        Checks the status code of the request response.

        Checks if the request response to get the token was successful.
        If yes, extracts the access token and stores it in the `token` instance variable.
        If not, displays an error in Streamlit with the status code.
        """
        if self.response_token.status_code == 200:
            self.token = self.response_token.json()["accessToken"]
        else:
            st.error(f"Erro ao obter o token. Status code: {self.response_token.status_code}")
            return None

    def auth_headers(self):
        """
        Creates the headers for the API requests with the Bearer token.
        
        This method creates a dictionary with the Authorization key and the value 
        of the Bearer token and the User-Agent key with the value "ASHER".
        It then returns the headers dictionary.
        """
        headers = {"Authorization": f"Bearer {self.token}", "User-Agent": "ASHER"}
        return headers

    def extract_transform_load_users(self):
        """
        Extracts, transforms, and loads user data from a CSV or XLSX file
        and makes API requests to get the limit information for each user.
        
        This method takes a CSV or XLSX file as an input, extracts the user data,
        makes an API request to get the limit information for each user,
        and stores the results in a new CSV file.
        
        If the API request fails or the response is not as expected, it displays
        an error in Streamlit.
        """
        try:
            st.title("Importação do usuário para realizar a consulta")
            uploaded_file = st.file_uploader("Envie seu arquivo CSV ou XLSX", type=["csv", "xlsx"])

            if not uploaded_file:
                st.warning("Envie um arquivo para começar.")
                return

            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file, sep=";", dtype="object")
            elif uploaded_file.name.endswith('.xlsx'):
                df = pd.read_excel(uploaded_file, dtype="object")
            else:
                st.error("Por favor, envie um arquivo no formato CSV ou XLSX.")
                return

            st.write("Pré-visualização dos dados:")
            st.dataframe(df.head(10))

            df["id_convenio"] = None
            for _, row in df.iterrows():
                cpf = row["CPF"].replace(".", "").replace("-", "")
                self.cheks_response_status()
                url_permitidos = f"{self.base_url}/consignado/v1/cliente/consulta-cpf?cpfRequest=00015867773"
                response = requests.get(url_permitidos, headers=self.auth_headers())
                
                if response.status_code == 200:
                    response_json = response.json()

                    if isinstance(response_json, list) and len(response_json) > 0:
                        data = response_json[0]
                        df.loc[df["CPF"] == row["CPF"], "id_convenio"] = data.get("idConvenio")
                    else:
                        st.warning(f"Resposta inesperada para CPF {cpf}.")
                else:
                    st.warning(f"Falha ao consultar CPF {cpf}. Status: {response.status_code}")

            rows = []
            self.cheks_response_status()
            for _, row in df.iterrows():
                cpf = row["CPF"]
                id_convenio = row["id_convenio"]

                if pd.isna(id_convenio):
                    st.warning(f"ID de convênio não encontrado para CPF {cpf}. Pulando...")
                    continue

                self.cheks_response_status()
                consult_limit = f"{self.base_url}/consignado/v1/limite/consultar/{cpf}/{id_convenio}"
                response = requests.get(consult_limit, headers=self.auth_headers())

                if response.status_code == 200:
                    response_json = response.json()
                    print(response_json)
                    if isinstance(response_json, list) and response_json:
                        for entry in response_json:
                            if not isinstance(entry, dict):
                                st.error(f"Resposta inesperada para CPF {cpf}. Entrada inválida: {entry}")
                                continue

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
                            print(base_info)
                            for opcao in entry.get("opcoes", []):
                                if isinstance(opcao, dict):
                                    row = base_info.copy()
                                    row.update(opcao)
                                    rows.append(row)
                                else:
                                    st.error(f"Opção inválida para CPF {cpf}: {opcao}")
                    else:
                        st.warning(f"Resposta inesperada para CPF {cpf}.")
                else:
                    st.warning(f"Falha ao consultar CPF {cpf}. Status: {response.status_code}")

            df.to_csv("generic_report.csv", sep=";", index=False)
            dff2 = pd.DataFrame(rows)
            dff2.drop_duplicates(
                subset=["cpf"],
                inplace=True,
            )
            output_file = "result.xlsx"
            dff2.to_excel(output_file, index=False)
            st.success(f"Arquivo salvo como {output_file}.")
            st.download_button(label="Baixar arquivo atualizado", data=open(output_file, "rb").read(), file_name=output_file, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            os.remove(output_file)

        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {e}")
            print(f"Error executing exception: {e}")

    def permiss_health_insurance(self):
        """
        Consulta os convênios permitidos via API e salva a resposta em um arquivo JSON.

        Se a API responder com sucesso, salva a resposta em um arquivo "convenios_permetidos.json"
        e exibe a resposta na tela.

        Se a API falhar, exibe um erro na tela.

        Retorna a resposta JSON se a API responder com sucesso, senão retorna None.
        """
        st.write("Consultar os convênios permitidos")
        if st.button("Consultar"):
            self.cheks_response_status()
            url_permitidos = f"https://api-parceiro.bancomaster.com.br/consignado/v1/cliente/convenio-permitido/"
            if url_permitidos:
                get_url_permitidos = requests.get(url_permitidos, headers=self.auth_headers())
                response_json = get_url_permitidos.json()
                with open("convenios_permetidos.json", "w") as json_file:
                    json.dump(response_json, json_file, indent=4)

                st.header("Resposta: ")
                st.json(response_json)
                return response_json
        return None


    def databasemange(self):
        st.write("Tratamento de dados para planilhas")

        buttons = "Remoçõa"


        if st.button("Remoção de dados duplicados"):
            st.success("Tratamento concluido")

        elif st.button("Concatenar dados"):
            st.text_input("""Insira o caminho da planilha com os dados a serem concatenados:""")

        elif st.button("Limpeza de dados"):
            st.text_input("""Insira o caminho da planilha com os dados a serem limpos:""")
        
        elif st.button("Cadastrar alguma coluna dentro arquivo"):
            st.text_input("""Insira o caminho da planilha com os dados a serem limpos:""")

    def run(self):
        """
        Configures the Streamlit page and handles user interaction.

        This method sets up the Streamlit page configuration for the "Banco Master" application,
        including the title and sidebar menu. It provides options for users to either 
        consult permitted agreements or process ETL. Based on user selection, it invokes
        the appropriate method to handle the request.

        - "Consultar Convênios": Calls `permiss_health_insurance` to consult permitted agreements.
        - "Processamento do ETL": Calls `extract_transform_load_users` to process ETL.
        
        Additionally, it adds a footer to the page.
        """
        st.set_page_config(page_title="Banco Master")
        st.title("Bot Banco Master")
        st.sidebar.title("Menu")
        menu_option = st.sidebar.selectbox("Selecione uma opção:", ["Consultar Convênios", "Processamento do ETL", "Tratamento de dados"])
        if menu_option == "Consultar Convênios":
            self.permiss_health_insurance()
        elif menu_option == "Processamento do ETL":
            self.extract_transform_load_users()
        
        elif menu_option == "Tratamento de dados":
            self.databasemange()
        
        add_footer()


if __name__ == "__main__":
    banco = BankerMaster()
    banco.run()
