# Arquitetura AWS - Pipeline Batch Bovespa

## Visão Geral

Este documento descreve a arquitetura AWS implementada para o Tech Challenge Fase 2, que consiste em um pipeline de dados completo para extrair, processar e analisar dados do pregão da B3 (Bovespa).

## Componentes da Arquitetura

### 1. Extração de Dados (Data Ingestion)

**Script de Scrap (b3_scraper.py)**
- Responsável por extrair dados da carteira teórica do IBovespa
- Executa diariamente para coletar dados atualizados
- Converte dados para formato Parquet
- Implementa particionamento por data (year/month/day)

**Tecnologias utilizadas:**
- Python 3.11
- BeautifulSoup4 para parsing HTML
- Pandas para manipulação de dados
- PyArrow para formato Parquet
- Boto3 para integração com AWS

### 2. Armazenamento de Dados Brutos (Raw Data Storage)

**Amazon S3 - Bucket Raw Data**
- Bucket: `bovespa-raw-data-bucket`
- Estrutura de particionamento: `raw-data/year=YYYY/month=MM/day=DD/`
- Formato: Parquet
- Configuração de eventos S3 para acionar Lambda

**Estrutura de dados:**
```
s3://bovespa-raw-data-bucket/
├── raw-data/
│   ├── year=2025/
│   │   ├── month=06/
│   │   │   ├── day=30/
│   │   │   │   └── ibov_carteira_20250630.parquet
│   │   │   └── day=31/
│   │   └── month=07/
│   └── year=2026/
```

### 3. Orquestração (Event-Driven Processing)

**AWS Lambda - Trigger Function**
- Função: `trigger-glue-job`
- Runtime: Python 3.11
- Acionada por eventos S3 (ObjectCreated)
- Responsável por iniciar jobs do AWS Glue
- Implementa filtros para processar apenas arquivos Parquet da pasta raw-data

**Configuração de Eventos:**
- Evento: `s3:ObjectCreated:*`
- Prefixo: `raw-data/`
- Sufixo: `.parquet`

### 4. Processamento ETL (Extract, Transform, Load)

**AWS Glue - Job ETL Visual**
- Job Name: `bovespa-etl-job`
- Modo: Visual (AWS Glue Studio)
- Runtime: Python 3.9 / Spark 3.3

**Transformações Implementadas:**

**A. Agrupamento e Sumarização:**
- Agrupamento por setor/tipo de ação
- Cálculo de soma total de quantidade teórica por grupo
- Cálculo de participação média por setor

**B. Renomeação de Colunas:**
- `qtde_teorica` → `quantidade_teorica_acoes`
- `participacao_pct` → `percentual_participacao_ibov`

**C. Cálculos com Data:**
- Cálculo de dias desde a última atualização
- Comparação com data de referência
- Criação de campo `dias_desde_coleta`

### 5. Armazenamento de Dados Refinados (Refined Data Storage)

**Amazon S3 - Bucket Refined Data**
- Bucket: `bovespa-refined-data-bucket` (pode ser o mesmo bucket com pasta diferente)
- Estrutura: `refined-data/year=YYYY/month=MM/day=DD/acao=CODIGO/`
- Formato: Parquet
- Particionamento duplo: por data e por código da ação

**Estrutura de dados refinados:**
```
s3://bovespa-refined-data-bucket/
├── refined-data/
│   ├── year=2025/
│   │   ├── month=06/
│   │   │   ├── day=30/
│   │   │   │   ├── acao=PETR4/
│   │   │   │   │   └── part-00000.parquet
│   │   │   │   ├── acao=VALE3/
│   │   │   │   │   └── part-00000.parquet
│   │   │   │   └── acao=ITUB4/
│   │   │   │       └── part-00000.parquet
```

### 6. Catalogação de Dados (Data Catalog)

**AWS Glue Data Catalog**
- Database: `default`
- Tabela: `bovespa_refined_data`
- Schema automático baseado nos dados Parquet
- Metadados atualizados automaticamente pelo job Glue

**Schema da Tabela:**
```sql
CREATE TABLE default.bovespa_refined_data (
  codigo string,
  acao string,
  tipo string,
  quantidade_teorica_acoes double,
  percentual_participacao_ibov double,
  data_pregao date,
  timestamp_coleta timestamp,
  dias_desde_coleta int
)
PARTITIONED BY (
  year int,
  month int,
  day int,
  acao string
)
STORED AS PARQUET
```

### 7. Consulta e Análise (Query Engine)

**Amazon Athena**
- Integração com Glue Data Catalog
- Consultas SQL sobre dados Parquet
- Interface web para análise ad-hoc
- Suporte a consultas complexas e agregações

**Exemplos de Consultas:**
```sql
-- Consulta básica
SELECT * FROM default.bovespa_refined_data 
WHERE year = 2025 AND month = 6 AND day = 30;

-- Análise por setor
SELECT tipo, 
       COUNT(*) as total_acoes,
       SUM(quantidade_teorica_acoes) as total_quantidade,
       AVG(percentual_participacao_ibov) as participacao_media
FROM default.bovespa_refined_data 
WHERE year = 2025 AND month = 6
GROUP BY tipo
ORDER BY total_quantidade DESC;

-- Evolução temporal
SELECT data_pregao,
       COUNT(*) as total_acoes,
       SUM(percentual_participacao_ibov) as participacao_total
FROM default.bovespa_refined_data 
WHERE year = 2025 AND month = 6
GROUP BY data_pregao
ORDER BY data_pregao;
```

### 8. Visualização (Opcional)

**Amazon Athena Notebooks**
- Jupyter notebooks integrados
- Visualizações com matplotlib/plotly
- Análises exploratórias interativas

## Fluxo de Dados

1. **Ingestão:** Script Python executa scrap diário dos dados da B3
2. **Armazenamento Raw:** Dados salvos em S3 formato Parquet com particionamento por data
3. **Trigger:** Evento S3 aciona função Lambda
4. **Processamento:** Lambda inicia job Glue ETL
5. **Transformação:** Glue aplica transformações e salva dados refinados
6. **Catalogação:** Glue atualiza Data Catalog automaticamente
7. **Consulta:** Dados disponíveis para consulta via Athena

## Configurações de Segurança

### IAM Roles e Políticas

**Lambda Execution Role:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns"
      ],
      "Resource": "*"
    }
  ]
}
```

**Glue Service Role:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::bovespa-*",
        "arn:aws:s3:::bovespa-*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:*"
      ],
      "Resource": "*"
    }
  ]
}
```

## Monitoramento e Logs

### CloudWatch Logs
- Logs da função Lambda
- Logs do job Glue ETL
- Métricas de execução

### CloudWatch Metrics
- Duração das execuções
- Taxa de sucesso/falha
- Volume de dados processados

## Custos Estimados

### Componentes de Custo:
- **S3 Storage:** ~$0.023/GB/mês
- **Lambda:** ~$0.20/1M requests + $0.0000166667/GB-segundo
- **Glue:** ~$0.44/DPU-hora
- **Athena:** ~$5.00/TB de dados escaneados

### Estimativa Mensal (dados diários):
- S3 Storage (1GB): ~$0.02
- Lambda (30 execuções): ~$0.01
- Glue (30 jobs, 5 min cada): ~$1.10
- Athena (consultas ocasionais): ~$0.50
- **Total estimado: ~$1.63/mês**

## Próximos Passos

1. **Implementação na AWS:**
   - Criar buckets S3
   - Configurar função Lambda
   - Criar job Glue visual
   - Configurar eventos S3

2. **Testes:**
   - Teste end-to-end do pipeline
   - Validação das transformações
   - Verificação das consultas Athena

3. **Otimizações:**
   - Ajuste de particionamento
   - Otimização de performance Glue
   - Configuração de lifecycle S3

4. **Monitoramento:**
   - Configurar alertas CloudWatch
   - Dashboard de monitoramento
   - Logs estruturados

