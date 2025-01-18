import streamlit as st
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

    def import_dataframe(self):
        try:
            st.write("Tratamento de arquivos")
            uploaded_file = st.file_uploader("Envie seu arquivo CSV ou XLSX", type=["csv", "xlsx"])

            if not uploaded_file:
                st.warning("Envie um arquivo para começar.")
                return

            # Lista de opções de tratamento de dados
            actions = [
                "Remoção de dados ausentes",
                "Concatenar dados",
                "Relatório de dados ausentes",
                "Separador de linhas",
                "Remover dados duplicados"
            ]

            selected_action = st.selectbox("Selecione uma opção:", actions)

            if selected_action == "Remover dados duplicados":
                st.write("Remoção de dados duplicados")
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
