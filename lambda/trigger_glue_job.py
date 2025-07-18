import json
import boto3
import logging
from datetime import datetime
from typing import Dict, Any

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clientes AWS
glue_client = boto3.client('glue')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Função Lambda que é acionada quando um novo arquivo é adicionado ao S3
    e inicia o job ETL do AWS Glue
    
    Args:
        event: Evento do S3 contendo informações sobre o arquivo adicionado
        context: Contexto de execução da Lambda
        
    Returns:
        Resposta da execução
    """
    
    try:
        logger.info(f"Evento recebido: {json.dumps(event)}")
        
        # Extrair informações do evento S3
        records = event.get('Records', [])
        
        if not records:
            logger.warning("Nenhum registro encontrado no evento")
            return {
                'statusCode': 400,
                'body': json.dumps('Nenhum registro encontrado no evento')
            }
        
        # Processar cada registro
        job_runs = []
        
        for record in records:
            # Verificar se é um evento S3
            if record.get('eventSource') != 'aws:s3':
                logger.warning(f"Evento não é do S3: {record.get('eventSource')}")
                continue
            
            # Extrair informações do bucket e objeto
            s3_info = record.get('s3', {})
            bucket_name = s3_info.get('bucket', {}).get('name')
            object_key = s3_info.get('object', {}).get('key')
            
            if not bucket_name or not object_key:
                logger.warning(f"Informações do S3 incompletas: bucket={bucket_name}, key={object_key}")
                continue
            
            logger.info(f"Processando arquivo: s3://{bucket_name}/{object_key}")
            
            # Verificar se o arquivo está na pasta raw-data e é um arquivo parquet
            if not object_key.startswith('raw-data/') or not object_key.endswith('.parquet'):
                logger.info(f"Arquivo ignorado (não é raw-data parquet): {object_key}")
                continue
            
            # Iniciar job do Glue
            job_run_id = start_glue_job(bucket_name, object_key)
            
            if job_run_id:
                job_runs.append({
                    'bucket': bucket_name,
                    'object_key': object_key,
                    'job_run_id': job_run_id
                })
        
        if job_runs:
            logger.info(f"Jobs do Glue iniciados: {len(job_runs)}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Jobs do Glue iniciados com sucesso',
                    'job_runs': job_runs
                })
            }
        else:
            logger.info("Nenhum job do Glue foi iniciado")
            return {
                'statusCode': 200,
                'body': json.dumps('Nenhum job do Glue foi iniciado')
            }
            
    except Exception as e:
        logger.error(f"Erro na execução da Lambda: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro interno: {str(e)}')
        }

def start_glue_job(bucket_name: str, object_key: str) -> str:
    """
    Inicia o job ETL do AWS Glue
    
    Args:
        bucket_name: Nome do bucket S3
        object_key: Chave do objeto no S3
        
    Returns:
        ID da execução do job ou None em caso de erro
    """
    
    try:
        # Nome do job Glue (deve ser configurado previamente)
        job_name = 'bovespa-etl-job'
        
        # Parâmetros para o job
        job_arguments = {
            '--input_bucket': bucket_name,
            '--input_key': object_key,
            '--output_bucket': bucket_name,
            '--output_prefix': 'refined-data/',
            '--enable-metrics': '',
            '--enable-continuous-cloudwatch-log': 'true',
            '--job-language': 'python'
        }
        
        # Iniciar o job
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=job_arguments
        )
        
        job_run_id = response['JobRunId']
        logger.info(f"Job do Glue iniciado: {job_name}, Run ID: {job_run_id}")
        
        return job_run_id
        
    except Exception as e:
        logger.error(f"Erro ao iniciar job do Glue: {str(e)}")
        return None

def get_date_from_s3_key(object_key: str) -> Dict[str, str]:
    """
    Extrai informações de data da chave do S3
    
    Args:
        object_key: Chave do objeto no S3 (ex: raw-data/year=2024/month=01/day=15/file.parquet)
        
    Returns:
        Dicionário com year, month, day
    """
    
    try:
        parts = object_key.split('/')
        date_info = {}
        
        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                if key in ['year', 'month', 'day']:
                    date_info[key] = value
        
        return date_info
        
    except Exception as e:
        logger.warning(f"Erro ao extrair data da chave S3: {str(e)}")
        return {}

