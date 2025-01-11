import os
import datetime
import altair as alt
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st
from database import Database

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(layout="wide", page_title="BATrânsito", page_icon=":taxi:")

db =  Database()
db.download_and_extract()
db.create_db()
db.populate_db()

st.title("Boletim de Acidente de Trânsito (BAT)")
st.markdown("## Preview das localizações dos acidentes")

def map(data, lat, lon, zoom, radius, escale, erange, color, pitch=35, cov=1):
    st.write(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state={
                "latitude": lat,
                "longitude": lon,
                "zoom": zoom,
                "pitch": pitch,
            },
            layers=[
                pdk.Layer(
                    "HexagonLayer",
                    data=data,
                    get_position=["lon", "lat"],
                    auto_highlight=True,
                    radius=radius,
                    elevation_scale=escale,
                    elevation_range=erange,
                    colorRange=color,
                    pickable=False,
                    extruded=True,
                    covarage=cov
                ),
            ],
        )
    )

@st.cache_data
def load_data(uf, limit):
    return db.fetch(
        f"""
        SELECT 
            COUNT(*) as qntd, 
            Latitude as lat, 
            Longitude as lon 
        FROM 
            Acidente ac
        LEFT JOIN Trecho t
            ON ac.TID = t.ID
        LEFT JOIN Municipio m
            ON t.MID = m.ID
        WHERE m.UF = '{uf}'
        GROUP BY 
            Latitude, Longitude 
        ORDER BY qntd DESC 
        LIMIT {limit}
        """
    )

def update_query_params():
    limit_selected = st.session_state["limit"]
    st.query_params["limit"] = limit_selected


if not st.session_state.get("url_synced", False):
    try:
        pickup_hour = int(st.query_params["limit"])
        st.session_state["limit"] = pickup_hour
        st.session_state["url_synced"] = True
    except KeyError:
        pass

estados = db.fetch("SELECT DISTINCT UF FROM Municipio")["UF"].to_list()

limit = st.slider(
        "Selecione a quantidade de acidentes", 1000, 10000, key="limit", on_change=update_query_params
    )

uf = st.radio(
    "Selecione um Estado",
    estados,
    horizontal=True,
    key="uf_map"
)

data = load_data(uf, limit)

data["qntd"] = data["qntd"].astype(int)
data["lat"] = pd.to_numeric(data["lat"].astype(str).str.replace(",", "."))
data["lon"] = pd.to_numeric(data["lon"].astype(str).str.replace(",", "."))

map(data, data["lat"].head().median(), data["lon"].head().median(), 10, 100, 2, [2000, 8000], [
        [255,255,178],
        [254,217,118],
        [254,178,76],
        [253,141,60],
        [240,59,32],
        [189,0,38],
    ])

# Consulta 1
st.markdown("## Quantidade de veículos por tipo envolvidos em acidentes")
st.dataframe(
    db.fetch(
        """SELECT tipo, COUNT(*) AS quantidade
         FROM Veiculo
         WHERE tipo != "Outros"
         GROUP BY tipo
         ORDER BY quantidade DESC
         """
    )
)

# Consulta 2
st.markdown("## Todos os municípios do Estado que já houve acidente")
uf2 = st.radio(
    "Selecione um Estado",
    estados,
    horizontal=True,
    key="consulta2"
)
st.dataframe(
    db.fetch(
        f"""SELECT Nome
         FROM Municipio
         WHERE UF = '{uf2}'
         ORDER BY Nome ASC
         """
    )
)

# Consulta 3
st.markdown("## Quais condições climáticas mais ocorrem acidentes")
st.dataframe(
    db.fetch(
        f"""
        SELECT C.Descricao as Condicao_Climatica,
        COUNT(A.ID) as Total_Acidentes
        FROM Acidente as A
        JOIN Condicao_climatica as C ON A.CID=C.ID
        GROUP BY C.Descricao
        ORDER BY Total_Acidentes DESC
         """
    )
)

# Consulta 4
st.markdown("## Quais os 10 km's de uma BR que mais ocorrem acidentes")
brs = db.fetch("SELECT DISTINCT Br FROM Trecho WHERE Br IS NOT NULL")["Br"].to_list()
br = st.radio(
    "Selecione uma Br",
    brs,
    horizontal=True,
    key="consulta4"
)
st.dataframe(
    db.fetch(
        f"""
        SELECT CAST(Trecho.km AS INTEGER) AS KM_Trecho, COUNT(*) AS Quantidade
        FROM Trecho
        JOIN Acidente ON Trecho.ID = Acidente.TID
        WHERE Trecho.br = {br}
        GROUP BY KM_Trecho
        ORDER BY Quantidade DESC
        LIMIT 10
         """
    )
)

# Consulta 5
st.markdown("## Quais delegacias registraram mais acidentes desde uma data")
data = st.date_input(
    "Escolha uma data de 2024", 
    min_value=datetime.date(2024, 1, 1),
    max_value=datetime.date(2024, 12, 31),
    format="DD/MM/YYYY"
)
st.dataframe(
    db.fetch(
        f"""
        SELECT Delegacia.ID, COUNT(*) AS Quantidade
        FROM Delegacia
        JOIN Acidente ac 
            ON Delegacia.ID = ac.DID
        WHERE ac.data > '{data}'
        GROUP BY Delegacia.ID
        ORDER BY Quantidade DESC
        LIMIT 10
         """
    )
)

# Consulta 6
st.markdown("## Probabilidades de acidentes com classificação escolhida ocorrerem em condições específicas")
todas_class = db.fetch("SELECT DISTINCT Classificacao FROM Acidente")["Classificacao"].to_list()
classificacao = st.radio(
    "Escolha uma classificação",
    todas_class,
    key="consulta5"
)
st.dataframe(
    db.fetch(
        f"""
        SELECT
        ac.Classificacao,
        cc.Descricao,
        cc.Fase_dia,
        tv.Tipo AS Tipo_trecho,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Acidente), 3) AS Probabilidade
        FROM
        Acidente ac
        LEFT JOIN Condicao_climatica cc
        ON ac.CID = cc.ID
        LEFT JOIN Trecho t
        ON ac.TID = t.ID
        LEFT JOIN Tracado_via tv
        ON tv.TID = t.ID
        WHERE
        tv.Tipo IS NOT NULL
        AND ac.Classificacao = '{classificacao}'
        GROUP BY
        ac.Classificacao,
        cc.Fase_dia,
        tv.Tipo,
        cc.Descricao
        ORDER BY
        Probabilidade DESC;
         """
    )
)

# Consulta 7
st.markdown("## Quais são os horários e fazes do dia que mais ocorreram acidentes com estado físico escolhido")
estados_fis = db.fetch("SELECT DISTINCT Estado_fisico FROM Envolveu_vitima")["Estado_fisico"].to_list()
estado_fis = st.radio(
    "Escolha um estado físico",
    estados_fis,
    key="consulta6"
)
st.dataframe(
    db.fetch(
        f"""
        SELECT Acidente.Horario, Condicao_climatica.Fase_dia, COUNT(*) AS Total_Estado
        FROM Acidente
        JOIN Envolveu_vitima ON Acidente.ID = Envolveu_vitima.AID
        JOIN Condicao_climatica ON Acidente.CID = Condicao_climatica.ID
        WHERE Envolveu_vitima.Estado_fisico = '{estado_fis}'
        GROUP BY Acidente.Horario, Condicao_climatica.Fase_dia
        ORDER BY Total_Estado DESC LIMIT 10;
         """
    )
)

# Consulta 8
st.markdown("## Quais modelos de veículo sofrem mais acidentes em dias chuvosos")
st.dataframe(
    db.fetch(
        f"""
        SELECT Veiculo.Marca, COUNT(*) AS Total_Acidentes
        FROM Veiculo
        JOIN Envolveu_veiculo ON Veiculo.ID = Envolveu_veiculo.VID
        JOIN Acidente ON Envolveu_veiculo.AID = Acidente.ID
        JOIN Condicao_climatica ON Acidente.CID = Condicao_climatica.ID
        WHERE Condicao_climatica.Descricao = 'Chuva'
        GROUP BY Veiculo.Marca
        ORDER BY Total_Acidentes DESC LIMIT 6
         """
    )
)

# Consulta 9
st.markdown("## Quais causas são as mais comuns por estado")
st.dataframe(
    db.fetch(
        f"""
        SELECT Estado, Causa, Total_Acidentes
        FROM(
            SELECT 
                M.UF as Estado, 
                C.Descricao as Causa, 
                COUNT(*) as Total_Acidentes, 
                ROW_NUMBER()OVER(PARTITION BY M.UF ORDER BY COUNT(*) DESC) as rn
            FROM Acidente as A
            JOIN Tem_causa as TC ON A.ID=TC.AID
            JOIN Causa as C ON TC.CID=C.ID
            JOIN Trecho as T ON A.TID=T.ID
            JOIN Municipio as M ON T.MID=M.ID
            GROUP BY M.UF, C.Descricao
        )subquery
        WHERE rn = 1
        ORDER BY Estado ASC
         """
    )
)

# Consulta 10
st.markdown("## Quais são as rodovias mais perigosas - mais acidentes fatais")
st.dataframe(
    db.fetch(
        f"""
        SELECT Br, COUNT(*) as Mortes
        FROM Acidente ac
        LEFT JOIN Envolveu_vitima ev
            ON ev.AID = ac.ID
        LEFT JOIN Trecho t
            ON t.ID = ac.TID
        WHERE Estado_fisico = 'Óbito'
        GROUP BY Br
        ORDER BY COUNT(*) DESC
        LIMIT 10;
        """
    )
)

coordenadas_br = db.fetch(
    """
    SELECT
        COUNT(*) as qntd,
        Latitude as lat,
        Longitude as lon
    FROM Acidente ac
    LEFT JOIN Trecho t
        ON t.ID = ac.TID
    WHERE t.Br IN (
        SELECT t2.Br
        FROM Acidente ac2
        LEFT JOIN Envolveu_vitima ev2
            ON ev2.AID = ac2.ID
        LEFT JOIN Trecho t2
            ON t2.ID = ac2.TID
        WHERE ev2.Estado_fisico = 'Óbito'
        GROUP BY t2.Br
        LIMIT 10
    )
    GROUP BY Latitude, Longitude
    """
)

coordenadas_br["qntd"] = coordenadas_br["qntd"].astype(int)
coordenadas_br["lat"] = pd.to_numeric(coordenadas_br["lat"].astype(str).str.replace(",", "."))
coordenadas_br["lon"] = pd.to_numeric(coordenadas_br["lon"].astype(str).str.replace(",", "."))

map(coordenadas_br, coordenadas_br["lat"].head().median(), coordenadas_br["lon"].head().median(), 3.5, 10000, 30, [1000, 30000], 
    [
        [251,106,74],
        [222,45,38],
        [165,15,21],
        [153,52,4],
        [99,99,99],
        [37,37,37],
    ], 60, 0.35
    ) 
# plotar o mapa indicando todas as coordenadas da tabela acidentes


