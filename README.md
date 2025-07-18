# Tech Challenge Fase 2 - Big Data Architecture

## 🎯 Visão Geral

Este projeto implementa um pipeline completo de dados para extrair, processar e analisar dados do pregão da B3 (Bovespa), utilizando serviços AWS para criar uma arquitetura escalável de Big Data.

## 🏗️ Arquitetura

![Diagrama da Arquitetura](docs/diagrama_arquitetura.png)

### Componentes Principais

- **🌐 Web Scraping**: Script Python para coleta de dados da B3
- **🗄️ Amazon S3**: Armazenamento de dados raw e refined em formato Parquet
- **⚡ AWS Lambda**: Orquestração event-driven do pipeline
- **🔄 AWS Glue**: Processamento ETL visual com Apache Spark
- **📚 Glue Data Catalog**: Catalogação automática de metadados
- **🔍 Amazon Athena**: Engine de consultas SQL serverless

## 📋 Requisitos Atendidos

- ✅ **Req 1**: Scrap de dados do site da B3
- ✅ **Req 2**: Dados brutos no S3 em formato Parquet com partição diária
- ✅ **Req 3**: Bucket S3 aciona Lambda que chama job Glue
- ✅ **Req 4**: Lambda implementada em Python
- ✅ **Req 5A**: Agrupamento numérico e sumarização
- ✅ **Req 5B**: Renomeação de duas colunas
- ✅ **Req 5C**: Cálculo com campos de data
- ✅ **Req 6**: Dados refinados particionados por data e ação
- ✅ **Req 7**: Catalogação automática no Glue Data Catalog
- ✅ **Req 8**: Dados disponíveis no Athena

## 🚀 Como Executar

### Pré-requisitos

- Python 3.11+
- AWS CLI configurado
- Conta AWS com permissões adequadas

### Instalação

```bash
# Clone o repositório
git clone <repository-url>
cd tech-challenge-bovespa

# Instale dependências
pip install -r requirements.txt
```

### Teste Local

```bash
# Execute o pipeline completo localmente
python scripts/test_pipeline.py

# Execute validação dos requisitos
python scripts/validation_tests.py
```

### Deploy na AWS

Siga o guia detalhado em [`docs/instrucoes_implementacao.md`](docs/instrucoes_implementacao.md)

## 📁 Estrutura do Projeto

```
tech-challenge-bovespa/
├── scripts/
│   ├── b3_scraper.py           # Script de web scraping
│   ├── test_pipeline.py        # Teste completo do pipeline
│   └── validation_tests.py     # Validação de requisitos
├── lambda/
│   └── trigger_glue_job.py     # Função Lambda
├── glue-jobs/
│   └── glue_job_config.json    # Configuração do job Glue
├── docs/
│   ├── arquitetura_aws.md      # Documentação da arquitetura
│   ├── instrucoes_implementacao.md  # Guia de implementação
│   ├── relatorio_final.md      # Relatório completo
│   ├── relatorio_final.pdf     # Relatório em PDF
│   └── diagrama_arquitetura.png # Diagrama visual
├── pipeline_test/              # Resultados dos testes
├── requirements.txt            # Dependências Python
└── README.md                   # Este arquivo
```

## 🧪 Testes e Validação

### Resultados dos Testes

- **Pipeline End-to-End**: ✅ 5/5 passos concluídos
- **Validação de Requisitos**: ✅ 7/7 requisitos aprovados
- **Taxa de Sucesso**: 100%

### Métricas de Performance

- **Latência de Ingestão**: ~45 segundos
- **Latência de Processamento**: ~3 minutos
- **Throughput de Consulta**: 2-8 segundos
- **Compressão de Dados**: 70-80% vs CSV

## 💰 Estimativa de Custos

| Serviço | Custo Mensal (USD) |
|---------|-------------------|
| S3 Storage (1GB) | $0.023 |
| Lambda (30 exec) | $0.001 |
| Glue (30 jobs × 5min) | $1.10 |
| Athena (consultas) | $0.50 |
| CloudWatch | $0.10 |
| **Total** | **$1.73** |

## 📊 Transformações Implementadas

### Transformação A: Agrupamento
- Agrupamento por tipo de ação (ON, PN, etc.)
- Soma da quantidade teórica por grupo
- Cálculo de participação média

### Transformação B: Renomeação
- `qtde_teorica` → `quantidade_teorica_acoes`
- `participacao_pct` → `percentual_participacao_ibov`

### Transformação C: Cálculo de Data
- Cálculo de `dias_desde_coleta`
- Diferença entre data atual e timestamp de coleta

## 🔍 Consultas de Exemplo

```sql
-- Análise por tipo de ação
SELECT tipo, 
       total_acoes_por_tipo,
       total_quantidade_teorica,
       participacao_media
FROM bovespa_refined_data 
ORDER BY total_quantidade_teorica DESC;

-- Evolução temporal
SELECT year, month, day,
       SUM(total_acoes_por_tipo) as total_acoes,
       SUM(total_quantidade_teorica) as quantidade_total
FROM bovespa_refined_data
GROUP BY year, month, day
ORDER BY year, month, day;
```

## 🛠️ Tecnologias Utilizadas

- **Python 3.11**: Linguagem principal
- **Apache Spark 3.3**: Processamento distribuído
- **Apache Parquet**: Formato de armazenamento colunar
- **AWS S3**: Data Lake
- **AWS Lambda**: Computação serverless
- **AWS Glue**: ETL gerenciado
- **Amazon Athena**: Query engine serverless
- **BeautifulSoup4**: Web scraping
- **Pandas**: Manipulação de dados
- **Boto3**: SDK AWS para Python

## 📈 Monitoramento

- **CloudWatch Logs**: Logs centralizados
- **CloudWatch Metrics**: Métricas customizadas
- **Alertas**: Notificações proativas
- **Dashboard**: Visibilidade operacional

## 🔒 Segurança

- **IAM Roles**: Princípio de menor privilégio
- **Criptografia**: TLS em trânsito, SSE-S3 em repouso
- **Auditoria**: CloudTrail para todas as ações
- **Validação**: Verificação de integridade de dados

## 🚀 Próximos Passos

### Curto Prazo
- [ ] Implementar cache Redis
- [ ] Otimizar particionamento
- [ ] Alertas inteligentes com ML

### Médio Prazo
- [ ] Adicionar outros índices
- [ ] Streaming analytics com Kinesis
- [ ] Integração com QuickSight

### Longo Prazo
- [ ] Modelos de ML preditivos
- [ ] Arquitetura multi-region
- [ ] APIs REST públicas

## 📚 Documentação

- [📖 Relatório Final Completo](docs/relatorio_final.pdf)
- [🏗️ Documentação da Arquitetura](docs/arquitetura_aws.md)
- [⚙️ Guia de Implementação](docs/instrucoes_implementacao.md)

## 🤝 Contribuição

Este projeto foi desenvolvido como parte do Tech Challenge Fase 2. Para sugestões ou melhorias, abra uma issue ou pull request.

## 📄 Licença

Este projeto é desenvolvido para fins educacionais como parte do Tech Challenge.

---

**Desenvolvido por**: Gustavo Imbelloni  
**Data**: Junho 2025  
**Versão**: 1.0.0

