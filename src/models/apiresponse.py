from sqlalchemy import create_engine, Column, Integer, String, Float, JSON, TIMESTAMP, func
from sqlalchemy.orm import DeclarativeBase
from controllers.datapaths import ManagePathDatabaseFiles

DATABASE_URL = f"sqlite:///{ManagePathDatabaseFiles().save_path_data()}/batch_processing.db"

class Base(DeclarativeBase):
    pass

class APIResponse(Base):
    __tablename__ = 'api_responses'
    id = Column(Integer, primary_key=True)
    cpf = Column(String(11), nullable=False)
    nome = Column(String(100))
    phone = Column(String(20))
    id_convenio = Column(String(50))
    matricula = Column(String(50))
    vl_multiplo_saque = Column(Float)
    limite_utilizado = Column(Float)
    limite_total = Column(Float)
    limite_disponivel = Column(Float)
    vl_limite_parcela = Column(Float)
    limite_parcela_utilizado = Column(Float)
    limite_parcela_disponivel = Column(Float)
    vl_margem = Column(Float)
    vl_multiplo_compra = Column(Float)
    vl_limite_compra = Column(Float)
    cd_banco = Column(String(20))
    cd_agencia = Column(String(20))
    cd_conta = Column(String(20))
    nao_perturbe = Column(String(5))
    saque_complementar = Column(String(5))
    refinanciamento = Column(JSON, nullable=False)
    numero_contrato = Column(String(20))
    vlMaximoParcelas = Column(Float)
    vlContrato = Column(Float)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())