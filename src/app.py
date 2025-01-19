import os
import streamlit as st
import shutil
from controllers.spreedsheets import SpreadSheets
from controllers.datapaths import ManagePathDatabaseFiles

class App:
    def __init__(self):
        pass
    
    def add_footer(self):
        """
        Add a footer to the Streamlit sidebar with the text "maisbs".
        
        This function adds a footer to the Streamlit sidebar with the text "maisbs".
        """
        st.sidebar.markdown(
            """
            <div style="text-align: center; padding: 10px; color: grey;">
                Automatizado de tabelas maisbs
            </div>
            """, 
            unsafe_allow_html=True
        )
        
    def remove_duplicates(self, uploaded_file):
        try:
            if st.button("Remocação de dados ausentes"):
                remove_duplicates = SpreadSheets(file_path=uploaded_file, file_type="csv", columns=[])
                remove_duplicates.handle_missing_data(drop=True)
                
        except Exception as e:
            st.error(f"Erro: {e}")

    def separator_lines(self, uploaded_file):
        qtd = st.text_input("Digite a quantidade de linhas para separar:")
        question_remove_duplicates = st.checkbox("Deseja remover dados ausentes antes da separação?")
        
        if not qtd.isdigit():
            st.warning("Por favor, insira um número válido.")
            return

        qtd = int(qtd)
        try:
            st.warning(f"Processando o arquivo: {uploaded_file.name}")

            file_type = "xlsx" if uploaded_file.name.endswith(".xlsx") else "csv"
            spreadsheet = SpreadSheets(file_path=uploaded_file, file_type=file_type, columns=[])
            
            if question_remove_duplicates:
                generic_reports = spreadsheet.generic_reports()
                st.write(f"Relatório geral de dados ausentes: {generic_reports}")
                self.remove_duplicates(uploaded_file)
            
            df = spreadsheet.load_data()
            st.write(f"Exibindo as primeiras {qtd} linhas:")
            st.dataframe(df.head(qtd))


            data_dir = ManagePathDatabaseFiles().save_path_data()

            file_paths = spreadsheet.remove_lines_data(
                data=df,
                chunk_size=qtd,
                output_path=data_dir,
                file_prefix="separator",
                encoding="utf-8",
                sep=";",
                index=False,
                file_type="csv"
            )

            st.success("Arquivos gerados com sucesso!")
            for path in file_paths:
                st.write(f"Arquivo gerado: {path}")
                with open(path, "rb") as file:
                    st.download_button(
                        label=f"Baixar {os.path.basename(path)}",
                        data=file,
                        file_name=os.path.basename(path),
                        mime="text/csv"
                    )
            if st.button("Apagar arquivos gerados"):
                try:
                    ManagePathDatabaseFiles().move_trash_files(move_trash=True)
                    st.success("Arquivos apagados com sucesso!")
                except Exception as e:
                    st.error(f"Erro inesperado: {e}")
                    
        except Exception as e:
            st.error(f"Erro ao executar: {e}")

    def inner_join(self, uploaded_file):
        _search_columns = st.text_input("Insira o nome das colunas para verificação (separadas por vírgula):").strip().lower()
        _questions_columns = st.text_input("Insira os nomes das colunas a incluir no resultado (separadas por vírgula):").strip().lower()

        try:
            st.warning("Cuidado ao importar o arquivo!")
            inner_file = st.file_uploader("Envie seu arquivo CSV ou XLSX", type=["csv", "xlsx"])
            
            if not uploaded_file:
                st.warning("Envie o arquivo principal para começar.")
                return

            if not _search_columns or not _questions_columns:
                st.warning("Insira os nomes das colunas para continuar.")
                return

            file_type = "xlsx" if uploaded_file.name.endswith(".xlsx") else "csv"
            spreadsheet = SpreadSheets(file_path=uploaded_file, file_type=file_type, columns=[])
            df = spreadsheet.load_data()
            st.write("Exibindo as primeiras 5 linhas do arquivo principal:")
            st.dataframe(df.head(5))

            search_columns = [col.strip() for col in _search_columns.split(",")]
            questions_columns = [col.strip() for col in _questions_columns.split(",")]

            if not inner_file:
                st.warning("Envie o segundo arquivo para realizar o join.")
                return
            
            inner_file_type = "xlsx" if inner_file.name.endswith(".xlsx") else "csv"
            inner_spreadsheet = SpreadSheets(file_path=inner_file, file_type=inner_file_type, columns=[])
            df2 = inner_spreadsheet.load_data()

            result_df = spreadsheet.search_columns_data(
                search_columns=search_columns,
                questions_columns=questions_columns,
                df2=df2
            )

            st.write("Resultado do Inner Join:")
            st.dataframe(result_df)

            output_path = "resultado.csv"
            result_df.to_csv(output_path, index=False)

            with open(output_path, "rb") as file:
                st.download_button(
                    label=f"Baixar {os.path.basename(output_path)}",
                    data=file,
                    file_name=os.path.basename(output_path),
                    mime="text/csv"
                )

            if st.button("Apagar arquivos gerados"):
                try:
                    ManagePathDatabaseFiles().move_trash_files(move_trash=True)
                    st.success("Arquivos apagados com sucesso!")
                except Exception as e:
                    st.error(f"Erro ao apagar arquivos: {e}")

        except Exception as e:
            st.error(f"Erro ao processar: {e}")

    def import_dataframe(self):
        try:
            st.write("Tratamento de arquivos")
            uploaded_file = st.file_uploader("Envie seu arquivo CSV ou XLSX", type=["csv", "xlsx"])

            if not uploaded_file:
                st.warning("Envie um arquivo para começar.")
                return

            actions = [
                "Escolha uma opção",
                "Separador de linhas",
                "Unir tabelas"
            ]

            selected_action = st.selectbox("Selecione uma opção:", actions)
            
            if selected_action == "Separador de linhas":
                self.separator_lines(uploaded_file=uploaded_file)

            elif selected_action == "Unir tabelas":
                self.inner_join(uploaded_file=uploaded_file)
 
        except Exception as e:
            st.error(f"Erro: {e}")

    def bankmaster(self):
        try:
            st.title("Importação de planilhas")
            uploaded_file = st.file_uploader("Envie seu arquivo CSV ou XLSX", type=["csv", "xlsx"])
            
            if not uploaded_file:
                st.warning("Envie um arquivo para começar.")
                return

        except Exception as e:
            st.write(f"Error: {e}")

    def home(self):
        st.title("Spreedsheet 🤖")

    def run(self):
        self.home()
        
        menu_option = st.sidebar.selectbox("Selecione uma opção:", ["Escolha uma opção", "Tratamento de dados", "Banco Master"])
        
        if menu_option == "Tratamento de dados":
            self.import_dataframe()

        elif menu_option == "Banco Master":
            self.bankmaster()

        self.add_footer()

if __name__ == "__main__":
    app = App()
    app.run()
