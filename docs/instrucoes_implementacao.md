# Guia de Implementação - Tech Challenge Fase 2

## Pré-requisitos

1. **Conta AWS ativa** com permissões para:
   - S3 (criar buckets, configurar eventos)
   - Lambda (criar funções, configurar triggers)
   - Glue (criar jobs, data catalog)
   - Athena (executar consultas)
   - IAM (criar roles e políticas)

2. **AWS CLI configurado** ou acesso ao Console AWS

3. **Conhecimento básico** de:
   - AWS S3
   - AWS Lambda
   - AWS Glue
   - Amazon Athena

## Passo 1: Configuração do Amazon S3

### 1.1 Criar Bucket para Dados Brutos

```bash
# Via AWS CLI
aws s3 mb s3://bovespa-raw-data-bucket- [SEU-ID-UNICO]

# Ou via Console AWS:
# 1. Acesse S3 Console
# 2. Clique em "Create bucket"
# 3. Nome: bovespa-raw-data-bucket-[SEU-ID-UNICO]
# 4. Região: us-east-1 (ou sua preferência)
# 5. Mantenha configurações padrão
# 6. Clique em "Create bucket"
```

### 1.2 Criar Estrutura de Pastas

```bash
# Criar estrutura de pastas no bucket
aws s3api put-object --bucket bovespa-raw-data-bucket-[SEU-ID-UNICO] --key raw-data/
aws s3api put-object --bucket bovespa-raw-data-bucket-[SEU-ID-UNICO] --key refined-data/
```

### 1.3 Configurar Versionamento (Opcional)

```bash
aws s3api put-bucket-versioning \
    --bucket bovespa-raw-data-bucket-[SEU-ID-UNICO] \
    --versioning-configuration Status=Enabled
```

## Passo 2: Configuração do IAM

### 2.1 Criar Role para Lambda

**Via Console AWS:**

1. Acesse IAM Console → Roles → Create role
2. Trusted entity: AWS service → Lambda
3. Permissions: Anexar as seguintes políticas:
   - `AWSLambdaBasicExecutionRole`
   - Política customizada (ver abaixo)

**Política customizada para Lambda:**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:BatchStopJobRun"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::bovespa-raw-data-bucket-[SEU-ID-UNICO]",
                "arn:aws:s3:::bovespa-raw-data-bucket-[SEU-ID-UNICO]/*"
            ]
        }
    ]
}
```

4. Nome da role: `lambda-glue-trigger-role`

### 2.2 Criar Role para Glue

**Via Console AWS:**

1. Acesse IAM Console → Roles → Create role
2. Trusted entity: AWS service → Glue
3. Permissions: Anexar as seguintes políticas:
   - `AWSGlueServiceRole`
   - Política customizada (ver abaixo)

**Política customizada para Glue:**

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
                "arn:aws:s3:::bovespa-raw-data-bucket-[SEU-ID-UNICO]",
                "arn:aws:s3:::bovespa-raw-data-bucket-[SEU-ID-UNICO]/*"
            ]
        }
    ]
}
```

4. Nome da role: `glue-bovespa-etl-role`

## Passo 3: Configuração do AWS Lambda

### 3.1 Criar Função Lambda

**Via Console AWS:**

1. Acesse Lambda Console → Create function
2. Author from scratch
3. Configurações:
   - Function name: `trigger-glue-bovespa-job`
   - Runtime: Python 3.11
   - Execution role: Use existing role → `lambda-glue-trigger-role`

### 3.2 Configurar Código da Lambda

1. No editor de código, substitua o conteúdo pelo código do arquivo `lambda/trigger_glue_job.py`
2. Ajuste a variável `job_name` para o nome do seu job Glue
3. Deploy

### 3.3 Configurar Trigger S3

1. Na função Lambda, clique em "Add trigger"
2. Configurações:
   - Trigger: S3
   - Bucket: bovespa-raw-data-bucket-[SEU-ID-UNICO]
   - Event type: All object create events
   - Prefix: raw-data/
   - Suffix: .parquet
3. Add

## Passo 4: Configuração do AWS Glue

### 4.1 Criar Database no Data Catalog

```bash
# Via AWS CLI
aws glue create-database \
    --database-input Name=bovespa_database,Description="Database para dados da Bovespa"

# Ou via Console:
# 1. Acesse Glue Console → Data Catalog → Databases
# 2. Add database
# 3. Nome: bovespa_database
# 4. Create
```

### 4.2 Criar Crawler para Dados Brutos (Opcional)

**Via Console AWS:**

1. Acesse Glue Console → Crawlers → Create crawler
2. Configurações:
   - Name: `bovespa-raw-data-crawler`
   - Data source: S3 → `s3://bovespa-raw-data-bucket-[SEU-ID-UNICO]/raw-data/`
   - IAM role: `glue-bovespa-etl-role`
   - Target database: `bovespa_database`
   - Table prefix: `raw_`
3. Create crawler
4. Run crawler

### 4.3 Criar Job ETL Visual

**Via Glue Studio:**

1. Acesse Glue Console → ETL Jobs → Visual ETL
2. Create job
3. Configurar nodes conforme o arquivo `glue-jobs/glue_job_config.json`:

**Node 1 - Data Source:**
- Type: S3
- S3 URL: `s3://bovespa-raw-data-bucket-[SEU-ID-UNICO]/raw-data/`
- Data format: Parquet

**Node 2 - Apply Mapping (Renomear Colunas):**
- Mapping:
  - `qtde_teorica` → `quantidade_teorica_acoes`
  - `participacao_pct` → `percentual_participacao_ibov`

**Node 3 - Custom Transform (Cálculo de Data):**
- Adicionar código Python para calcular dias desde coleta

**Node 4 - Aggregate (Agrupamento):**
- Group by: `tipo`, `year`, `month`, `day`
- Aggregations:
  - `quantidade_teorica_acoes`: sum → `total_quantidade_teorica`
  - `percentual_participacao_ibov`: avg → `participacao_media`
  - `codigo`: count → `total_acoes_por_tipo`

**Node 5 - Data Target:**
- Type: S3
- Format: Parquet
- S3 URL: `s3://bovespa-raw-data-bucket-[SEU-ID-UNICO]/refined-data/`
- Partition keys: `year`, `month`, `day`, `tipo`
- Data Catalog: Update table in data catalog
- Database: `bovespa_database`
- Table name: `bovespa_refined_data`

4. Job details:
   - Name: `bovespa-etl-job`
   - IAM Role: `glue-bovespa-etl-role`
   - Glue version: 4.0
   - Worker type: G.1X
   - Number of workers: 2

5. Save job

## Passo 5: Teste do Pipeline

### 5.1 Executar Script de Scrap

```bash
# No ambiente local ou EC2
cd tech-challenge-bovespa
python3 scripts/b3_scraper.py --s3-bucket bovespa-raw-data-bucket-[SEU-ID-UNICO]
```

### 5.2 Verificar Execução

1. **S3:** Verificar se arquivo foi criado em `raw-data/year=YYYY/month=MM/day=DD/`
2. **Lambda:** Verificar logs no CloudWatch
3. **Glue:** Verificar execução do job no console
4. **Data Catalog:** Verificar se tabela foi criada/atualizada

## Passo 6: Configuração do Amazon Athena

### 6.1 Configurar Query Result Location

1. Acesse Athena Console
2. Settings → Manage
3. Query result location: `s3://bovespa-raw-data-bucket-[SEU-ID-UNICO]/athena-results/`
4. Save

### 6.2 Executar Consultas de Teste

```sql
-- Verificar dados disponíveis
SELECT * FROM bovespa_database.bovespa_refined_data LIMIT 10;

-- Análise por tipo de ação
SELECT 
    tipo,
    total_acoes_por_tipo,
    total_quantidade_teorica,
    participacao_media
FROM bovespa_database.bovespa_refined_data
WHERE year = 2025 AND month = 6
ORDER BY total_quantidade_teorica DESC;

-- Evolução temporal
SELECT 
    year, month, day,
    SUM(total_acoes_por_tipo) as total_acoes,
    SUM(total_quantidade_teorica) as quantidade_total
FROM bovespa_database.bovespa_refined_data
GROUP BY year, month, day
ORDER BY year, month, day;
```

## Passo 7: Monitoramento e Alertas

### 7.1 Configurar CloudWatch Alarms

**Para Lambda:**
```bash
aws cloudwatch put-metric-alarm \
    --alarm-name "Lambda-TriggerGlue-Errors" \
    --alarm-description "Alert on Lambda errors" \
    --metric-name Errors \
    --namespace AWS/Lambda \
    --statistic Sum \
    --period 300 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --dimensions Name=FunctionName,Value=trigger-glue-bovespa-job \
    --evaluation-periods 1
```

**Para Glue:**
```bash
aws cloudwatch put-metric-alarm \
    --alarm-name "Glue-Job-Failures" \
    --alarm-description "Alert on Glue job failures" \
    --metric-name glue.driver.aggregate.numFailedTasks \
    --namespace Glue \
    --statistic Sum \
    --period 300 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --evaluation-periods 1
```

### 7.2 Dashboard CloudWatch (Opcional)

1. Acesse CloudWatch Console → Dashboards
2. Create dashboard: `Bovespa-Pipeline-Dashboard`
3. Adicionar widgets para:
   - Lambda invocations e errors
   - Glue job runs e duration
   - S3 object count

## Passo 8: Automação Diária

### 8.1 Configurar EventBridge para Execução Diária

```bash
# Criar regra para execução diária às 18:00 UTC
aws events put-rule \
    --name "daily-bovespa-scraping" \
    --schedule-expression "cron(0 18 * * ? *)" \
    --description "Trigger daily Bovespa data scraping"

# Adicionar target (Lambda function)
aws events put-targets \
    --rule "daily-bovespa-scraping" \
    --targets "Id"="1","Arn"="arn:aws:lambda:us-east-1:ACCOUNT-ID:function:trigger-glue-bovespa-job"
```

### 8.2 Configurar Permissões EventBridge

```bash
aws lambda add-permission \
    --function-name trigger-glue-bovespa-job \
    --statement-id "allow-eventbridge" \
    --action "lambda:InvokeFunction" \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:us-east-1:ACCOUNT-ID:rule/daily-bovespa-scraping"
```

## Troubleshooting

### Problemas Comuns

1. **Lambda timeout:** Aumentar timeout para 5 minutos
2. **Glue job falha:** Verificar permissões IAM e paths S3
3. **Athena não encontra dados:** Executar `MSCK REPAIR TABLE` ou recriar crawler
4. **S3 event não dispara:** Verificar configuração de prefixo/sufixo

### Logs Importantes

- **Lambda:** CloudWatch Logs → `/aws/lambda/trigger-glue-bovespa-job`
- **Glue:** CloudWatch Logs → `/aws-glue/jobs/logs-v2/`
- **S3:** CloudTrail para eventos de bucket

## Custos Estimados

### Componentes:
- **S3:** ~$0.023/GB/mês
- **Lambda:** ~$0.20/1M requests
- **Glue:** ~$0.44/DPU-hora
- **Athena:** ~$5.00/TB escaneado

### Estimativa mensal (execução diária):
- S3 (1GB): $0.02
- Lambda (30 execuções): $0.01
- Glue (30 jobs × 5min): $1.10
- Athena (consultas): $0.50
- **Total: ~$1.63/mês**

## Próximos Passos

1. **Otimização:** Ajustar particionamento e compressão
2. **Segurança:** Implementar encryption at rest/transit
3. **Backup:** Configurar lifecycle policies S3
4. **Alertas:** Expandir monitoramento
5. **Documentação:** Criar runbooks operacionais

