#!/usr/bin/env python
# coding: utf-8

# # Teste

# In[ ]:


import boto3 

def processar_arquivos_cext(bucket_origem, bucket_destino):
    s3 = boto3.client('s3')

    objetos = s3.list_objects_v2(Bucket=bucket_origem)

    if 'Contents' in objetos:
        for objeto in objetos['Contents']:
            nome_arquivo = objeto['Key']
            
            if nome_arquivo.startswith('CEXT_756') and nome_arquivo.endswith('.CCB'):
                try:
                    response = s3.get_object(Bucket=bucket_origem, Key=nome_arquivo)
                    conteudo_arquivo = response['Body'].read().decode('utf-8')
                    s3.put_object(Bucket=bucket_destino, Key=nome_arquivo, Body=conteudo_arquivo)
                    print(f"Arquivo {nome_arquivo} processado e gravado com sucesso no Bucket Bronze.")
                
                except Exception as e:
                    print(f"Erro ao processar o arquivo {nome_arquivo}: {str(e)}")
    
    else:
        print("Não foram encontrados arquivos no bucket de origem.")

bucket_origem = 'volumes/dev/tiintegracao/team/cartoes/cext'
bucket_destino = 'bucket-bronze'

processar_arquivos_cext(bucket_origem, bucket_destino)



# In[ ]:


def validar_estrutura_arquivo(caminho_arquivo):
    try:
        with open(caminho_arquivo, 'r') as arquivo:
            linhas = arquivo.readlines()
            if not linhas[0].startswith("CEXT0"):
                return False, "Estrutura do cabeçalho inválida"

            if not linhas[-1].startswith("CEXT9"):
                return False, "Estrutura do trailer inválida"
            return True, "Estrutura do arquivo válida"
    except Exception as e:
        return False, f"Erro ao validar estrutura do arquivo: {str(e)}"

def criar_tabela_log(status, mensagem):
    try:
      
        data_inicio = datetime.datetime.now()
        data_fim = datetime.datetime.now()
   
        print(f"Data e hora de início: {data_inicio}")
        print(f"Data e hora de fim: {data_fim}")
        print(f"Status: {status}")
        print(f"Mensagem: {mensagem}")
        return True
    except Exception as e:
        return False, f"Erro ao criar tabela de log: {str(e)}"

caminho_arquivo = "CEXT_7562011_20240125_0002504.CCB"
status_validacao, mensagem_validacao = validar_estrutura_arquivo(caminho_arquivo)
if status_validacao:
    status_log = "Sucesso"
    mensagem_log = "Arquivo CEXT validado com sucesso"
else:
    status_log = "Erro"
    mensagem_log = f"Falha ao validar arquivo CEXT: {mensagem_validacao}"

criar_tabela_log(status_log, mensagem_log)


# In[ ]:


def identificar_conta_ailos(linha):

    numero_cartao = linha[6:25].strip()
    
    if numero_cartao:  
        resultado_consulta = consultar_cartao_ailos(numero_cartao)
        if resultado_consulta:
            return True  
    return False  

def consultar_cartao_ailos(numero_cartao):
 
    return True 

def gravar_dados_bucket_silver(nome_arquivo, data_arquivo, conteudo_linhas):
    try:
        id_arquivo_controle = buscar_id_arquivo_controle(nome_arquivo)
        id_arquivo = gerar_identificador()
        data_arquivo_formatada = datetime.datetime.strptime(data_arquivo, '%Y%m%d').strftime('%d/%m/%Y')
        
        print("Inserindo dados na tabela CARTOES.TB_ARQUIVO:")
        print(f"IDARQUIVO: {id_arquivo}")
        print(f"IDARQUIVO_CONTROLE: {id_arquivo_controle}")
        print(f"NMARQUIVO: {nome_arquivo}")
        print(f"DTARQUIVO: {data_arquivo_formatada}")
        print(f"DHREGISTRO: {datetime.datetime.now()}")

        for linha in conteudo_linhas:
            id_arquivo_linha = gerar_identificador()
            print("Inserindo dados na tabela CARTOES.TB_ARQUIVO_LINHA:")
            print(f"IDARQUIVO_LINHA: {id_arquivo_linha}")
            print(f"IDARQUIVO: {id_arquivo}")
            print(f"NRLINHA: {conteudo_linhas.index(linha) + 1}")  
            print(f"DSCONTEUDO: {linha.strip()}")  
            print(f"DTPROCESSO: {datetime.datetime.now()}")  
            print("CDSITUACAO: 1")  
        return True
    except Exception as e:
        return False, f"Erro ao gravar dados no Bucket Silver: {str(e)}"

def buscar_id_arquivo_controle(nome_arquivo):
    return 2  

def gerar_identificador():
    return "ID_UNIQUE_PLACEHOLDER"


status_gravacao, mensagem_gravacao = gravar_dados_bucket_silver(nome_arquivo, data_arquivo, conteudo_linhas)
if status_gravacao:
    print("Dados gravados no Bucket Silver com sucesso!")
else:
    print(f"Falha ao gravar dados no Bucket Silver: {mensagem_gravacao}")



# In[ ]:


import requests 

def job_databricks():
    try:
        url = "https://seu_databricks_url/api/2.0/jobs/run-now"

        token = "seu_token_de_acesso"

        parametros = {
            "job_id": "seu_job_id",
            "notebook_params": {
                "parametro1": "valor1",
                "parametro2": "valor2"
            }
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        response = requests.post(url, json=parametros, headers=headers)

        if response.status_code == 200:
            print("JOB disparado com sucesso!")
        else:
            print(f"Falha ao disparar o JOB: {response.text}")
    except Exception as e:
        print(f"Erro ao disparar o JOB: {str(e)}")

disparar_job_databricks()


# In[ ]:


import cx_Oracle
import boto3
import datetime

def bronze(nome_arquivo, data_arquivo):
    connection = cx_Oracle.connect()
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO GOLD_TABLE (ID_ARQUIVO, ID_ARQUIVO_CONTROLE, NM_ARQUIVO, DT_ARQUIVO, DT_REGISTRO) 
    VALUES (:id_arquivo, :id_arquivo_controle, :nome_arquivo, TO_DATE(:data_arquivo, 'DD/MM/YYYY'), SYSDATE)
    """
    cursor.execute(insert_query, {'id_arquivo': id_arquivo, 'id_arquivo_controle': id_arquivo_controle, 
                                   'nome_arquivo': nome_arquivo, 'data_arquivo': data_arquivo})
    connection.commit()
    cursor.close()
    connection.close()
    pass

def silver(id_arquivo_controle, nome_arquivo, data_arquivo):
    connection = cx_Oracle.connect()
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO GOLD_TABLE (ID_ARQUIVO, ID_ARQUIVO_CONTROLE, NM_ARQUIVO, DT_ARQUIVO, DT_REGISTRO) 
    VALUES (:id_arquivo, :id_arquivo_controle, :nome_arquivo, TO_DATE(:data_arquivo, 'DD/MM/YYYY'), SYSDATE)
    """
    cursor.execute(insert_query, {'id_arquivo': id_arquivo, 'id_arquivo_controle': id_arquivo_controle, 
                                   'nome_arquivo': nome_arquivo, 'data_arquivo': data_arquivo})
    connection.commit()
    cursor.close()
    connection.close()
    pass

def gold(id_arquivo, id_arquivo_controle, nome_arquivo, data_arquivo):
    connection = cx_Oracle.connect()
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO GOLD_TABLE (ID_ARQUIVO, ID_ARQUIVO_CONTROLE, NM_ARQUIVO, DT_ARQUIVO, DT_REGISTRO) 
    VALUES (:id_arquivo, :id_arquivo_controle, :nome_arquivo, TO_DATE(:data_arquivo, 'DD/MM/YYYY'), SYSDATE)
    """
    cursor.execute(insert_query, {'id_arquivo': id_arquivo, 'id_arquivo_controle': id_arquivo_controle, 
                                   'nome_arquivo': nome_arquivo, 'data_arquivo': data_arquivo})
    connection.commit()
    cursor.close()
    connection.close()

gravar_dados_bronze(nome_arquivo, data_arquivo)
gravar_dados_silver(id_arquivo_controle, nome_arquivo, data_arquivo)
gravar_dados_gold(id_arquivo, id_arquivo_controle, nome_arquivo, data_arquivo)


# In[ ]:


curl -X POST -H "Authorization: Bearer <>" -H "Content-Type: application/json" -d '{ "job_id": "https://seu_databricks_url/api/2.0/jobs/run-now", "notebook_params": { "parametro1": " ", "parametro2": "" } }' 

