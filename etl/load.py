from psycopg2.extras import execute_values
from db.connection import connect_dw
import pandas as pd


def insert_indicadores(df):
    conn = connect_dw()
    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS homologacao.fvitrine_indicadores_loja (
                    id INT4 PRIMARY KEY,
                    company_id VARCHAR(50),
                    name VARCHAR(255),
                    date_start timestamp,
                    date_end timestamp,
                    store_id VARCHAR(50),
                    nm_store VARCHAR(255),
                    code INT2,
                    nm_group VARCHAR(255),
                    store_status VARCHAR(30),
                    APROVADOR VARCHAR(30),
                    EXECUTOR VARCHAR(50),
                    COMERCIAL VARCHAR(30),
                    VISUALIZADOR VARCHAR(20),
                    FRANQUEADO VARCHAR(50),
                    totalSpace INT2,
                    totalDone INT2,
                    totalNotApply INT2,
                    done INT2,
                    notApply INT2,
                    notDone INT2,
                    executionDate timestamp,
                    onTime NUMERIC(5,2),
                    storeExecutionTime VARCHAR(20),
                    storeApprovalTime VARCHAR(20),
                    notEvaluatedOnTime INT2,
                    notEvaluatedOffTime INT2,
                    notSendOnTime INT2,
                    notSendOffTime INT2,
                    quality NUMERIC(5,2),
                    rework NUMERIC(5,2)
                );
            """)

            #cursor.execute("TRUNCATE TABLE homologacao.fvitrine_indicadores_loja;")
            print("Tabela 'homologacao.fvitrine_indicadores_loja' criada e limpa.")

            cols = list(df.columns)

            values = [
                tuple(None if pd.isna(v) else v for v in row)
                for row in df.to_numpy()
            ]

            insert_query = f"""
                INSERT INTO homologacao.fvitrine_indicadores_loja ({','.join(cols)})
                VALUES %s
                ON CONFLICT (id) DO NOTHING;
            """

            execute_values(cursor, insert_query, values)
            conn.commit()
            print(f"{len(df)} registros inseridos com sucesso na tabela 'homologacao.fvitrine_indicadores_loja'.")

    except Exception as e:
        conn.rollback()
        print(f"Erro durante a operação no DW: {e}")
    finally:
        conn.close()

def insert_quality(df):
    conn = connect_dw()
    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS homologacao.fvitrine_quality_by_space (
                    store_id varchar(50),
                    space_id varchar(50),
                    name varchar(150),
                    orientation TEXT,
                    star numeric(5,2),
                    quality numeric(5,2),
                    notApply numeric(5,2),
                    notApplyReasons TEXT,
                    approvedReasons TEXT,
                    reprovedReasons TEXT,
                    PRIMARY KEY (store_id, space_id)
                );
            """)

            #cursor.execute("TRUNCATE TABLE homologacao.fvitrine_quality_by_space;")
            print("Tabela 'homologacao.fvitrine_quality_by_space' criada e limpa.")

            df = df.drop_duplicates(subset=["store_id", "space_id"])

            cols = list(df.columns)

            values = [
                tuple(None if pd.isna(v) else v for v in row)
                for row in df.to_numpy()
            ]

            insert_query = f"""
                INSERT INTO homologacao.fvitrine_quality_by_space ({','.join(cols)})
                VALUES %s
                ON CONFLICT (store_id, space_id) DO NOTHING;
            """

            execute_values(cursor, insert_query, values)
        conn.commit()
        print(f"{len(df)} registros inseridos com sucesso na tabela 'homologacao.fvitrine_quality_by_space'.")

    except Exception as e:
        conn.rollback()
        print(f"Erro durante a operação no DW: {e}")
    finally:
        conn.close()