# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer, String, Table, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class FieldEntity(Base):
    __tablename__ = 'FieldEntity'

    field = Column(String(100), primary_key=True, nullable=False, unique=True)
    entity = Column(String(100), primary_key=True, nullable=False)

    querys = relationship('KqlQuery', secondary='QueryField')


class KqlQuery(Base):
    __tablename__ = 'KqlQuery'

    source_path = Column(String(1000), nullable=False)
    query = Column(Text(10000))
    name = Column(String(100))
    query_id = Column(Integer, primary_key=True)
    local_path = Column(String(1000), nullable=False)


class QueryAttribute(Base):
    __tablename__ = 'QueryAttribute'

    query_id = Column(ForeignKey('KqlQuery.query_id'), primary_key=True, nullable=False)
    attribute_name = Column(String(100), primary_key=True, nullable=False)
    attribute_value = Column(String(1000))

    query = relationship('KqlQuery')


# t_QueryField = Table(
#     'QueryField', metadata,
#     Column('query_id', ForeignKey('KqlQuery.query_id'), primary_key=True, nullable=False),
#     Column('field', ForeignKey('FieldEntity.field'), primary_key=True, nullable=False, unique=True)
# )
class QueryField(Base):
    __tablename__ = "QueryField"

    query_id = Column(ForeignKey('KqlQuery.query_id'), primary_key=True, nullable=False)
    field = Column(ForeignKey('FieldEntity.field'), primary_key=True, nullable=False, unique=True)

    query = relationship('KqlQuery')
    entity = relationship("FieldEntity")


class QueryFunction(Base):
    __tablename__ = 'QueryFunction'

    query_id = Column(ForeignKey('KqlQuery.query_id'), primary_key=True, nullable=False)
    function = Column(String(100), primary_key=True, nullable=False)

    query = relationship('KqlQuery')


class QueryOperator(Base):
    __tablename__ = 'QueryOperator'

    query_id = Column(ForeignKey('KqlQuery.query_id'), primary_key=True, nullable=False, unique=True)
    operator = Column(String(100), primary_key=True, nullable=False)

    query = relationship('KqlQuery', uselist=False)


class QueryTable(Base):
    __tablename__ = 'QueryTable'

    table_name = Column(String(100), primary_key=True, nullable=False, unique=True)
    query_id = Column(ForeignKey('KqlQuery.query_id'), primary_key=True, nullable=False)

    query = relationship('KqlQuery')


class OperatorFieldReference(Base):
    __tablename__ = 'OperatorFieldReference'

    query_id = Column(ForeignKey('QueryOperator.query_id'), primary_key=True, nullable=False)
    field = Column(ForeignKey('QueryField.field'), primary_key=True, nullable=False)
    operator = Column(String(100), primary_key=True, nullable=False)

    QueryField = relationship('QueryField')
    query = relationship('QueryOperator')


class OperatorTableReference(Base):
    __tablename__ = 'OperatorTableReference'

    query_id = Column(ForeignKey('QueryOperator.query_id'), primary_key=True, nullable=False)
    operator = Column(String(100), primary_key=True, nullable=False)
    table_name = Column(ForeignKey('QueryTable.table_name'), primary_key=True, nullable=False)

    query = relationship('QueryOperator')
    QueryTable = relationship('QueryTable')
