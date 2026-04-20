import psycopg2
import requests
import pandas as pd
import os 
from dotenv import load_dotenv
from datetime import datetime
from psycopg2.extras import execute_values 
import loading


#-----------------------------------------------------------------------------------------------------------------------------------------#
#EXTRAÇÃO E PROCESSMENTO DOS DADOS
#-----------------------------------------------------------------------------------------------------------------------------------------#
load_dotenv()

url = "https://api.vitrineretail.app/api/indicatorsStores/exportXlsxNew"

token = os.getenv("API_TOKEN")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

hoje = datetime.today().strftime("%Y-%m-%d")

payload = {
    "filters": {
        "format": "json",
        "filterType": "store",
        "manuals": [],
        "groups": [],
        "stores": [],
        "spaces": [],
        "categories": [],
        "responsibles": [],
        "comercials": [],
        "viewers": [],
        "startDate": "2026-01-01",
        "endDate": hoje
    }
}

response = requests.post(url, headers=headers, json=payload)

if response.status_code == 200:
    data = response.json()
   
    df = pd.DataFrame(data)

    df_main = df.drop(columns=['qualityBySpace'], errors="ignore")

    cols_int = [
        "done", "notApply", "notDone",
        "notEvaluatedOnTime", "notEvaluatedOffTime",
        "code"
    ]

    for col in cols_int:
        df_main[col] = pd.to_numeric(df_main[col], errors="coerce")

    df_main["executionDate"] = pd.to_datetime(
        df_main["executionDate"],
        dayfirst=True,
        errors="coerce"
    )

    cols_float = ["onTime", "quality", "rework"]

    for col in cols_float:
        df_main[col] = pd.to_numeric(df_main[col], errors="coerce")


    #print(df_main.columns)

    rows = []

    for _, row in df.iterrows():
        qspaces = row.get("qualityBySpace")

        if not qspaces:
            continue

        if isinstance(qspaces, dict):
            for space_id, space_data in qspaces.items():
                rows.append({
                    "store_id": row["store_id"],
                    "space_id": space_id,
                    **space_data
                })

        elif isinstance(qspaces, list):
            for space_data in qspaces:
                rows.append({
                    "store_id": row["store_id"],
                    **space_data
                })
    

    df_quality = pd.DataFrame(rows)

    cols_float = ["star", "quality", "notApply"]

    for col in cols_float:
        df_quality[col] = pd.to_numeric(df_quality[col], errors="coerce")

    df_quality = df_quality.replace("", None)


else:
    print("Erro: ", response.status_code)
    print(response.text)

#-----------------------------------------------------------------------------------------------------------------------------------------#
#CONEXÃO COM DW
#-----------------------------------------------------------------------------------------------------------------------------------------#
def conectar_DW():
    # Conectar ao banco de dados
    print(os.getenv("DW_HOST"))
    try:
        conn = psycopg2.connect(
            database = os.getenv("DW_NAME"),
            user = os.getenv("DW_USER"),
            host = os.getenv("DW_HOST"),
            password = os.getenv("DW_PASS"), 
            port = os.getenv("DW_PORT")
        )
        print("Conexão com o banco de dados estabelecida com sucesso.")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
    return conn

def desconectar(conn):
    if conn:
        conn.close()
        print("Conexão com o banco de dados encerrada.")
    else:
        print("Nenhuma conexão ativa para encerrar.")

#-----------------------------------------------------------------------------------------------------------------------------------------#
#CRIAÇÃO DAS TABELAS
#-----------------------------------------------------------------------------------------------------------------------------------------#
def put_indicadores_loja(df):
    conn = conectar_DW()
    if conn is None:
        return None

    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS homologacao.fvitrine_indicadores_loja (
                id INT4 PRIMARY KEY,
                company_id VARCHAR(50) NULL,
                name VARCHAR(255) NULL,
                date_start timestamp NULL,
                date_end timestamp NULL,
                store_id VARCHAR(50) NULL,
                nm_store VARCHAR(255) NULL,
                code INT2 NULL,
                nm_group VARCHAR(255) NULL,
                store_status VARCHAR(30) NULL,
                APROVADOR VARCHAR(30) NULL,
                EXECUTOR VARCHAR(50) NULL,
                COMERCIAL VARCHAR(30) NULL,
                VISUALIZADOR VARCHAR(20) NULL,
                FRANQUEADO VARCHAR(50) NULL,
                totalSpace INT2 NULL,
                totalDone INT2 NULL,
                totalNotApply INT2 NULL,
                done INT2 NULL,
                notApply INT2 NULL,
                notDone INT2 NULL,
                executionDate timestamp NULL,
                onTime NUMERIC(5,2) NULL,
                storeExecutionTime VARCHAR(20) NULL,
                storeApprovalTime VARCHAR(20) NULL,
                notEvaluatedOnTime INT2 NULL,
                notEvaluatedOffTime INT2 NULL,
                notSendOnTime INT2 NULL,
                notSendOffTime INT2 NULL,
                quality NUMERIC(5,2) NULL,
                rework NUMERIC(5,2) NULL
            );
        """
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            # Limpa a tabela antes de inserir novos dados para evitar duplicatas
            cursor.execute("TRUNCATE TABLE homologacao.fvitrine_indicadores_loja;")
            print("Tabela 'homologacao.fvitrine_indicadores_loja' criada e limpa.")

            # 3. Prepara para inserção em massa
            cols = ','.join(list(df.columns))
            # Cria a query de insert com placeholders %s
            insert_query = f"INSERT INTO homologacao.fvitrine_indicadores_loja ({cols}) VALUES %s ON CONFLICT (id) DO NOTHING;"

            # Converte o DataFrame para uma lista de tuplas
            values = [
                tuple(None if pd.isna(v) else v for v in row)
                for row in df.to_numpy()
            ]
            # 4. Executa a inserção em massa
            execute_values(cursor, insert_query, values)

        conn.commit()
        print(f"{len(df)} registros inseridos com sucesso na tabela 'homologacao.fvitrine_indicadores_loja'.")

    except Exception as e:
        print(f"Erro durante a operação no DW: {e}")
        conn.rollback() 
    finally:
        desconectar(conn)

def put_quality_by_space(df):
    conn = conectar_DW()
    if conn is None:
        return None

    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS homologacao.fvitrine_quality_by_space (
                store_id  varchar(50) NULL,
                space_id varchar(50) NULL,
                name varchar(150) NULL,
                orientation TEXT NULL,
                star numeric(5,2) NULL,
                quality numeric(5,2) NULL,
                notApply numeric(5,2) NULL,
                notApplyReasons TEXT NULL,
                approvedReasons TEXT NULL,
                reprovedReasons TEXT NULL,
                PRIMARY KEY (store_id, space_id)
            );
        """
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            # Limpa a tabela antes de inserir novos dados para evitar duplicatas
            cursor.execute("TRUNCATE TABLE homologacao.fvitrine_quality_by_space;")
            print("Tabela 'homologacao.fvitrine_quality_by_space' criada e limpa.")

            # 3. Prepara para inserção em massa
            cols = ','.join(list(df.columns))
            # Cria a query de insert com placeholders %s
            insert_query = f"INSERT INTO homologacao.fvitrine_quality_by_space ({cols}) VALUES %s ON CONFLICT (store_id, space_id) DO NOTHING;"

            # Converte o DataFrame para uma lista de tuplas
            values = [
                tuple(None if pd.isna(v) else v for v in row)
                for row in df.to_numpy()
            ]

            # 4. Executa a inserção em massa
            execute_values(cursor, insert_query, values)

        conn.commit()
        print(f"{len(df)} registros inseridos com sucesso na tabela 'homologacao.fvitrine_quality_by_space'.")

    except Exception as e:
        print(f"Erro durante a operação no DW: {e}")
        conn.rollback() # Desfaz a transação em caso de erro
    finally:
        desconectar(conn)

#put_indicadores_loja(df_main)
put_quality_by_space(df_quality)