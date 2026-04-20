import psycopg2
import os 

def connect_dw():
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

def disconnect(conn):
    if conn:
        conn.close()
        print("Conexão com o banco de dados encerrada.")
    else:
        print("Nenhuma conexão ativa para encerrar.")