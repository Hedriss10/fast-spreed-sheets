import os
from sqlalchemy import Column, Integer, String, TIMESTAMP, Text
from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, Float, JSON, TIMESTAMP, func, Boolean
from sqlalchemy.orm import DeclarativeBase
from dotenv import load_dotenv

load_dotenv()

DBHOST = os.getenv("DBHOST")
DBPORT = os.getenv("DBPORT")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("DBPASSWORD")

DATABASE_URL = f"postgresql://{USERNAME}:{PASSWORD}@{DBHOST}:{DBPORT}/{DATABASE}"

class Base(DeclarativeBase):
    pass

class APIResponse(Base):
    __tablename__ = 'api_responses'
    __table_args__ = {'schema': 'spreed_sheets'}
    id = Column(Integer, primary_key=True)
    cpf = Column(String(100), nullable=False)
    nome = Column(String(100))
    phone = Column(String(100))
    id_convenio = Column(String(50))
    matricula = Column(String(100))
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
    cd_banco = Column(String(50))  
    cd_agencia = Column(String(50))  
    cd_conta = Column(String(50))  
    nao_perturbe = Column(Boolean) 
    saque_complementar = Column(Boolean) 
    refinanciamento = Column(JSON, nullable=False)
    numero_contrato = Column(String(100))
    vlMaximoParcelas = Column(Float)
    vlContrato = Column(Float)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())

class Loggger(Base):
    __tablename__ = 'loggers'
    __table_args__ = {'schema': 'spreed_sheets'}
    id = Column(Integer, primary_key=True)
    message = Column(Text, nullable=False)
    exception = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())

class User(Base):
    __tablename__ = 'owners_cpf'
    __table_args__ = {'schema': 'spreed_sheets'}
    id = Column(Integer, primary_key=True)
    cpf = Column(Text, nullable=False)
    
    
class UserFinancialAgreements(Base):
    __tablename__ = 'financial_agreements'
    __table_args__ = {'schema': 'spreed_sheets'}
    id = Column(Integer, primary_key=True)
    cpf = Column(Text, nullable=False)
    id_convenio = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    
    
class ReportGeneric(Base):
    __tablename__ = 'report_generic'
    __table_args__ = {'schema': 'spreed_sheets'}
    id = Column(Integer, primary_key=True)
    cpf = Column(Text, nullable=False)
    id_convenio = Column(Text, nullable=True)
    message = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())