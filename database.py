import os
import csv
import zipfile
import sqlite3
import requests
import pandas as pd
import seaborn as sns
from io import BytesIO
import matplotlib.pyplot as plt
from collections import Counter


class Database:
    complete = False
    nome_zip = "acidentes2024.zip"
    nome_csv = "acidentes2024_todas_causas_tipos.csv"
    db_name = 'acidentes2024.db'
    
    def __init__(self):
        if not os.path.isfile(self.db_name):
            open(self.db_name, "w").close()
            os.chmod(self.db_name, 0o666)            
        
        self.conn = sqlite3.connect(self.db_name)

    def fetch(self, query, formatted=True):
        # execute the query and fetch all rows
        cur = self.conn.cursor()
        cur.execute(query)
        rs = cur.fetchall()

        # extract column names from the cursor description
        columns = [desc[0] for desc in cur.description]

        # return a dataframe with column names
        return pd.DataFrame(rs, columns=columns) if formatted else rs


    def show_tables(self):
        return [x[0] for x in self.fetch('SELECT tbl_name FROM sqlite_master WHERE type="table"', False)]


    def shape(self, table):
        nrows = self.fetch(f'SELECT COUNT(*) FROM {table}', False)[0][0]
        ncols = self.fetch(f'SELECT COUNT(*) FROM pragma_table_info("{table}")', False)[0][0]

        return (nrows, ncols)


    def desc(self, table):
        cur = self.conn.cursor()
        cur.execute(f'PRAGMA table_info("{table}")')
        columns = [row[1] for row in cur.fetchall()]

        return columns


    def info(self, table):
        # table constraints (domain, null, default, pk)
        df1 = self.fetch(f'PRAGMA table_info("{table}")')
        columns = self.desc(table)

        # entries per column
        counts = ', '.join([f'COUNT(*) AS "{column}"' for column in columns])
        df2 = self.fetch(f'SELECT {counts} FROM "{table}"').transpose()
        df2.columns = ['count']

        # non-null entries per column
        counts = ', '.join([f'COUNT("{column}") AS "{column}"' for column in columns])
        df3 = self.fetch(f'SELECT {counts} FROM "{table}"').transpose()
        df3.columns = ['notnull count']

        # unique non-null entries per column
        counts = ', '.join([f'COUNT(DISTINCT "{column}") AS "{column}"' for column in columns])
        df4 = self.fetch(f'SELECT {counts} FROM "{table}"').transpose()
        df4.columns = ['unique count']

        return df1.merge(df2, left_on='name', right_index=True) \
                .merge(df3, left_on='name', right_index=True) \
                .merge(df4, left_on='name', right_index=True)
        

    def download_and_extract(self):
        if not os.path.isfile(self.nome_zip):
            url = "https://drive.usercontent.google.com/u/0/uc?id=14qBOhrE1gioVtuXgxkCJ9kCA8YtUGXKA&export=download"
            response = requests.get(url, stream=True)

            with open(self.nome_zip, "wb") as fileZip:
                for chunk in response.iter_content(chunk_size=262144):
                    fileZip.write(chunk)

        if not os.path.isfile(self.nome_csv):
            # Abrindo e extraindo o arquivo ZIP
            with zipfile.ZipFile(self.nome_zip, 'r') as zip_ref:
                zip_ref.extractall("./")
        
        try:
            if 'Acidente' in self.show_tables():
                self.complete = True
            else:
                os.remove(self.db_name)
                open(self.db_name, "w").close()
                os.chmod(self.db_name, 0o666)
                
        except Exception:
            pass


    def create_db(self):
        if self.complete:
            return
        
        cur = self.conn.cursor()

        # Configurações PRAGMA para ingestão mais rápida
        cur.execute('PRAGMA synchronous = OFF')
        cur.execute('PRAGMA journal_mode = MEMORY')

        # Tamanho do lote
        BATCH_SIZE = 1000

        # Função para limpar valores
        def limpar_valores(linha):
            """
            Substitui valores específicos como 'NA' por None.
            """
            return [None if value in ("NA", "N/A", "", "NA/NA") else value.strip() for value in linha]

        # Começa a transação para inserção em massa
        cur.execute('BEGIN TRANSACTION')

        # Lê o arquivo CSV e insere os dados
        with open(self.nome_csv, 'r', encoding="latin-1") as f:
            reader = csv.reader(f, delimiter=';')  # Define o delimitador como ';'
            header = next(reader)  # Lê o cabeçalho

            # Cria a tabela dinamicamente
            columns = [f'"{column.strip().replace(" ", "_")}"' for column in header]
            create = f'CREATE TABLE IF NOT EXISTS Source ({", ".join(columns)})'
            cur.execute(create)

            # Prepara a instrução de inserção
            placeholders = ', '.join(['?'] * len(header))
            insert = f'INSERT INTO Source VALUES ({placeholders})'

            # Processa as linhas em lotes
            batch = []
            for row in reader:
                batch.append(limpar_valores(row))  # Limpa os valores na linha
                if len(batch) == BATCH_SIZE:
                    cur.executemany(insert, batch)
                    batch = []

            # Insere quaisquer linhas restantes
            if batch:
                cur.executemany(insert, batch)

        # Commit das mudanças
        self.conn.commit()

        # Restaura as configurações PRAGMA para segurança
        cur.execute('PRAGMA synchronous = FULL')
        cur.execute('PRAGMA journal_mode = DELETE')

        # Correcoes-----------------------------------------------

        # Definindo os tipos corretos
        cur.execute('UPDATE Source SET idade = CAST(idade AS INTEGER) WHERE idade IS NOT NULL') # idade -> int
        cur.execute("UPDATE Source SET km = CAST(REPLACE(km, ',', '.') AS REAL) WHERE km IS NOT NULL;") # km replace(',', '.') -> float
        cur.execute("""UPDATE Source SET causa_principal = CASE
                    WHEN causa_principal = 'Sim' THEN 1
                    WHEN causa_principal = 'Não' THEN 0
                    END;""") # convert causa_principal to boolean


        # Excluindo dados não-confiáveis
        cur.execute('DELETE FROM Source WHERE idade IS NULL AND idade > 116') # excluir linhas com idade > 116 e Null (Pessoa mais velha do mundo tem 116 anos)
        cur.execute('DELETE FROM Source WHERE pesid IS NULL OR id_veiculo IS NULL OR tipo_envolvido IS NULL') # excluir linhas com pesid, id_veiculo ou tipo_envolvido nulos

        # Criando tabelas------------------------------------------------

        # tabela Acidente
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Acidente (
            ID INTEGER PRIMARY KEY,
            Data DATE NOT NULL,
            Horario TIME NOT NULL,
            Latitude REAL,
            Longitude REAL,
            Classificacao,
            TID INTEGER,
            CID INTEGER,
            DID INTEGER,
            FOREIGN KEY (TID) REFERENCES Trecho(ID),
            FOREIGN KEY (CID) REFERENCES Condicao_climatica(ID),
            FOREIGN KEY (DID) REFERENCES Delegacia(ID)
        )
        """)

        # tabela Veiculo
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Veiculo (
            ID INTEGER PRIMARY KEY,
            Tipo TEXT NOT NULL,
            Marca TEXT NOT NULL,
            Ano_fabricacao INTEGER NOT NULL
        )
        """)

        # tabela Vitima
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Vitima (
            ID INTEGER PRIMARY KEY,
            Sexo TEXT
        )
        """)

        # tabela Trecho
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Trecho (
            ID INTEGER PRIMARY KEY,
            Area_urbana  BOOLEAN ,
            Br TEXT ,
            Km INT ,
            Tipo_pista TEXT ,
            Sentido_via TEXT ,
            MID INTEGER,
            FOREIGN KEY (MID) REFERENCES Municipio(ID)
        )
        """)

        # tabela Tracado_via
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Tracado_via (
            TID INTEGER,
            Tipo TEXT,
            PRIMARY KEY (TID, Tipo),
            FOREIGN KEY (TID) REFERENCES Trecho(ID)
        )
        """)

        # tabela Municio
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Municipio (
            ID INTEGER PRIMARY KEY,
            Nome TEXT,
            UF TEXT
        )
        """)

        # tabela Clima
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Condicao_climatica (
            ID INTEGER PRIMARY KEY,
            Fase_dia TEXT,
            Descricao TEXT
        )
        """)

        # tabela Delegacia
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Delegacia (
            ID TEXT PRIMARY KEY,
            Regional TEXT,
            UOP TEXT
        )
        """)

        # tabela Causa
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Causa (
            ID INTEGER PRIMARY KEY,
            Descricao TEXT
        )
        """)

        # tabela envolveu veiculo
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Envolveu_veiculo(
            VID INTEGER,
            AID INTEGER,
            PRIMARY KEY (VID, AID),
            FOREIGN KEY (VID) REFERENCES Veiculo(ID),
            FOREIGN KEY (AID) REFERENCES Acidente(ID)
        )
        """)

        # tabela envolveu vitima
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Envolveu_vitima(
            PID INTEGER,
            AID INTEGER,
            Idade INTEGER,
            Estado_fisico TEXT,
            PRIMARY KEY (PID, AID),
            FOREIGN KEY (PID) REFERENCES Vitima(ID),
            FOREIGN KEY (AID) REFERENCES Acidente(ID)
        )
        """)

        # tabela tem causa
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Tem_causa(
        AID INTEGER,
        CID INTEGER,
        Principal BOOLEAN,
        PRIMARY KEY (AID, CID),
        FOREIGN KEY (AID) REFERENCES Acidente(ID),
        FOREIGN KEY (CID) REFERENCES Causa(ID)
        )
        """)
        
        self.conn.commit()


    def populate_db(self):
        if self.complete:
            return
        
        cur = self.conn.cursor()
        
        # tabela causa
        cur.execute("""
        INSERT INTO Causa (Descricao)
        SELECT DISTINCT causa_acidente
        FROM Source
        WHERE causa_acidente IS NOT NULL
        """)

        # tabela veiculo
        cur.execute("""
        INSERT INTO Veiculo (ID, Tipo, Marca, Ano_fabricacao)
        SELECT DISTINCT id_veiculo, tipo_veiculo, marca, ano_fabricacao_veiculo
        FROM Source
        WHERE id_veiculo IS NOT NULL
        """)

        # tabela vitima
        cur.execute("""
        INSERT INTO Vitima (ID, Sexo)
        SELECT DISTINCT pesid, sexo
        FROM Source
        WHERE pesid IS NOT NULL
        """)

        # tabela clima
        cur.execute("""
        INSERT INTO Condicao_climatica (Fase_dia, Descricao)
        SELECT DISTINCT fase_dia, condicao_metereologica
        FROM Source
        """)

        # tabela delegacia
        cur.execute("""
        INSERT INTO Delegacia (ID, Regional, UOP)
        SELECT delegacia, regional, uop
        FROM Source
        WHERE delegacia IS NOT NULL
        GROUP BY delegacia
        """)

        # tabela municipio
        cur.execute("""
        INSERT INTO Municipio (Nome, UF)
        SELECT DISTINCT municipio, uf
        FROM Source
        WHERE municipio IS NOT NULL AND uf IS NOT NULL
        """)

        # tabela trecho
        cur.execute("""
        INSERT INTO Trecho (Area_urbana, Br, Km, Tipo_pista, Sentido_via, MID)
        SELECT DISTINCT
            CASE WHEN s.uso_solo = "Sim" THEN 1 ELSE 0 END,
            s.br,
            s.km,
            s.tipo_pista,
            s.sentido_via,
            m.ID
        FROM Source s
        LEFT JOIN Municipio m
        ON m.Nome = s.municipio AND
        m.uf = s.uf
        WHERE s.uso_solo IS NOT NULL
        AND s.br IS NOT NULL
        AND s.km IS NOT NULL
        AND s.tipo_pista IS NOT NULL
        AND s.sentido_via IS NOT NULL
        """)

        #tabela Tracado_via
        cur.execute("""
        SELECT DISTINCT s.tracado_via, t.ID
        FROM Source s
        LEFT JOIN Municipio m
        ON m.Nome = s.municipio
        AND m.UF = s.uf
        LEFT JOIN Trecho t
        ON t.Area_urbana = CASE
                                WHEN s.uso_solo = 'Sim' THEN TRUE
                                WHEN s.uso_solo = 'Não' THEN FALSE
                                ELSE NULL
                            END
        AND t.Br = s.br
        AND t.Km = s.km
        AND t.Tipo_pista = s.tipo_pista
        AND t.Sentido_via = s.sentido_via
        AND t.MID = m.ID
        WHERE s.tracado_via IS NOT NULL
        """)

        # tabela acidente
        cur.execute("""
        INSERT INTO Acidente (ID, Data, Horario, Latitude, Longitude, Classificacao, TID, CID, DID)
        SELECT DISTINCT
            s.id AS SourceID,
            s.data_inversa AS Data,
            s.horario AS Horario,
            s.latitude,
            s.longitude,
            s.classificacao_acidente AS Classificacao,
            t.ID AS TID,
            c.ID AS CID,
            d.ID AS DID
        FROM Source s
        LEFT JOIN Municipio m
        ON m.Nome = s.municipio
        AND m.UF = s.uf
        LEFT JOIN Trecho t
        ON t.Br = s.br
        AND t.Km = s.km
        AND t.Sentido_via = s.sentido_via
        AND t.Tipo_pista = s.tipo_pista
        AND t.Area_urbana = CASE
                                WHEN s.uso_solo = 'Sim' THEN TRUE
                                WHEN s.uso_solo = 'Não' THEN FALSE
                                ELSE NULL
                            END
        AND t.MID = m.ID
        LEFT JOIN Tracado_via tv
        ON tv.TID = t.ID
        LEFT JOIN Condicao_climatica c
        ON c.Descricao = s.condicao_metereologica
        AND c.Fase_dia = s.fase_dia
        LEFT JOIN Delegacia d
        ON d.ID = s.delegacia
        WHERE s.id IS NOT NULL
        AND s.data_inversa IS NOT NULL
        AND s.horario IS NOT NULL
        AND s.classificacao_acidente IS NOT NULL
        """)

        # tabela envolveu veiculo
        cur.execute("""
        INSERT INTO Envolveu_veiculo (AID, VID)
        SELECT DISTINCT id, id_veiculo
        FROM Source
        """)

        # tabela envolveu vitima
        cur.execute("""
        INSERT INTO Envolveu_vitima (PID, AID, Idade, Estado_fisico)
        SELECT DISTINCT pesid, id, idade, estado_fisico
        FROM Source

        """)

        # tabela tem causa
        cur.execute("""
        INSERT INTO Tem_causa (AID, CID, Principal)
        SELECT DISTINCT Source.id, Causa.ID, Source.causa_principal
        FROM Source, Causa
        WHERE Source.causa_acidente = Causa.Descricao

        """)

        # Excluindo Source---------------------------------------
        cur.execute("DROP TABLE Source")
        self.conn.commit()


if __name__ == '__main__':
    db = Database()
    db.download_and_extract()
    db.create_db()
    db.populate_db()
    
    data = db.fetch("SELECT Latitude, Longitude FROM Acidente")
    print(data)
    