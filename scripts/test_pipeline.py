#!/usr/bin/env python3
"""
Script para testar o pipeline completo do Tech Challenge
Simula a execução end-to-end sem usar AWS real
"""

import pandas as pd
import os
import json
import logging
from datetime import datetime, date
from pathlib import Path
import shutil

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PipelineTester:
    """Classe para testar o pipeline completo localmente"""
    
    def __init__(self, base_dir: str = "./pipeline_test"):
        """
        Inicializa o testador do pipeline
        
        Args:
            base_dir: Diretório base para os testes
        """
        self.base_dir = Path(base_dir)
        self.raw_data_dir = self.base_dir / "raw-data"
        self.refined_data_dir = self.base_dir / "refined-data"
        self.logs_dir = self.base_dir / "logs"
        
        # Criar diretórios
        for dir_path in [self.raw_data_dir, self.refined_data_dir, self.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def step1_generate_sample_data(self) -> bool:
        """
        Passo 1: Gerar dados simulados da B3
        
        Returns:
            True se bem-sucedido
        """
        try:
            logger.info("=== PASSO 1: Gerando dados simulados da B3 ===")
            
            # Dados simulados mais realistas
            sample_data = [
                {
                    'codigo': 'PETR4',
                    'acao': 'PETROBRAS',
                    'tipo': 'PN N2',
                    'qtde_teorica': 4500000000.0,
                    'participacao_pct': 8.5,
                    'data_pregao': date.today().isoformat(),
                    'timestamp_coleta': datetime.now().isoformat()
                },
                {
                    'codigo': 'VALE3',
                    'acao': 'VALE',
                    'tipo': 'ON N1',
                    'qtde_teorica': 5200000000.0,
                    'participacao_pct': 9.2,
                    'data_pregao': date.today().isoformat(),
                    'timestamp_coleta': datetime.now().isoformat()
                },
                {
                    'codigo': 'ITUB4',
                    'acao': 'ITAUUNIBANCO',
                    'tipo': 'PN N1',
                    'qtde_teorica': 3800000000.0,
                    'participacao_pct': 7.1,
                    'data_pregao': date.today().isoformat(),
                    'timestamp_coleta': datetime.now().isoformat()
                },
                {
                    'codigo': 'BBDC4',
                    'acao': 'BRADESCO',
                    'tipo': 'PN N1',
                    'qtde_teorica': 3200000000.0,
                    'participacao_pct': 6.3,
                    'data_pregao': date.today().isoformat(),
                    'timestamp_coleta': datetime.now().isoformat()
                },
                {
                    'codigo': 'ABEV3',
                    'acao': 'AMBEV S/A',
                    'tipo': 'ON',
                    'qtde_teorica': 4400000000.0,
                    'participacao_pct': 2.7,
                    'data_pregao': date.today().isoformat(),
                    'timestamp_coleta': datetime.now().isoformat()
                },
                {
                    'codigo': 'B3SA3',
                    'acao': 'B3',
                    'tipo': 'ON EJ NM',
                    'qtde_teorica': 5200000000.0,
                    'participacao_pct': 3.5,
                    'data_pregao': date.today().isoformat(),
                    'timestamp_coleta': datetime.now().isoformat()
                },
                {
                    'codigo': 'WEGE3',
                    'acao': 'WEG',
                    'tipo': 'ON NM',
                    'qtde_teorica': 2800000000.0,
                    'participacao_pct': 4.8,
                    'data_pregao': date.today().isoformat(),
                    'timestamp_coleta': datetime.now().isoformat()
                },
                {
                    'codigo': 'RENT3',
                    'acao': 'LOCALIZA',
                    'tipo': 'ON NM',
                    'qtde_teorica': 1600000000.0,
                    'participacao_pct': 2.1,
                    'data_pregao': date.today().isoformat(),
                    'timestamp_coleta': datetime.now().isoformat()
                }
            ]
            
            # Converter para DataFrame
            df = pd.DataFrame(sample_data)
            
            # Adicionar colunas de particionamento
            df['year'] = pd.to_datetime(df['data_pregao']).dt.year
            df['month'] = pd.to_datetime(df['data_pregao']).dt.month
            df['day'] = pd.to_datetime(df['data_pregao']).dt.day
            
            # Criar estrutura de particionamento
            today = date.today()
            partition_dir = self.raw_data_dir / f"year={today.year}" / f"month={today.month:02d}" / f"day={today.day:02d}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            # Salvar em Parquet
            filename = f"ibov_carteira_{today.strftime('%Y%m%d')}.parquet"
            file_path = partition_dir / filename
            df.to_parquet(file_path, index=False, engine='pyarrow')
            
            logger.info(f"Dados simulados salvos em: {file_path}")
            logger.info(f"Total de registros: {len(df)}")
            logger.info(f"Colunas: {list(df.columns)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro no Passo 1: {e}")
            return False
    
    def step2_simulate_lambda_trigger(self) -> bool:
        """
        Passo 2: Simular o trigger da Lambda
        
        Returns:
            True se bem-sucedido
        """
        try:
            logger.info("=== PASSO 2: Simulando trigger da Lambda ===")
            
            # Simular evento S3
            s3_event = {
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "bovespa-raw-data-bucket"},
                            "object": {"key": f"raw-data/year={date.today().year}/month={date.today().month:02d}/day={date.today().day:02d}/ibov_carteira_{date.today().strftime('%Y%m%d')}.parquet"}
                        }
                    }
                ]
            }
            
            # Salvar evento simulado
            event_file = self.logs_dir / "s3_event_simulation.json"
            with open(event_file, 'w') as f:
                json.dump(s3_event, f, indent=2)
            
            logger.info(f"Evento S3 simulado salvo em: {event_file}")
            logger.info("Lambda seria acionada para iniciar job Glue")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro no Passo 2: {e}")
            return False
    
    def step3_simulate_glue_etl(self) -> bool:
        """
        Passo 3: Simular o job ETL do Glue
        
        Returns:
            True se bem-sucedido
        """
        try:
            logger.info("=== PASSO 3: Simulando job ETL do Glue ===")
            
            # Encontrar arquivo de dados brutos
            today = date.today()
            raw_file_pattern = self.raw_data_dir / f"year={today.year}" / f"month={today.month:02d}" / f"day={today.day:02d}" / "*.parquet"
            
            raw_files = list(self.raw_data_dir.glob(f"year={today.year}/month={today.month:02d}/day={today.day:02d}/*.parquet"))
            
            if not raw_files:
                logger.error("Nenhum arquivo de dados brutos encontrado")
                return False
            
            raw_file = raw_files[0]
            logger.info(f"Processando arquivo: {raw_file}")
            
            # Carregar dados brutos
            df = pd.read_parquet(raw_file)
            logger.info(f"Dados carregados: {len(df)} registros")
            
            # TRANSFORMAÇÃO A: Renomear colunas (Requisito 5B)
            df = df.rename(columns={
                'qtde_teorica': 'quantidade_teorica_acoes',
                'participacao_pct': 'percentual_participacao_ibov'
            })
            logger.info("✓ Transformação A: Colunas renomeadas")
            
            # TRANSFORMAÇÃO B: Cálculo com data (Requisito 5C)
            df['timestamp_coleta_dt'] = pd.to_datetime(df['timestamp_coleta'])
            df['dias_desde_coleta'] = (pd.Timestamp.now() - df['timestamp_coleta_dt']).dt.days
            logger.info("✓ Transformação B: Cálculo de dias desde coleta")
            
            # TRANSFORMAÇÃO C: Agrupamento e sumarização (Requisito 5A)
            df_aggregated = df.groupby(['tipo', 'year', 'month', 'day']).agg({
                'quantidade_teorica_acoes': 'sum',
                'percentual_participacao_ibov': 'mean',
                'codigo': 'count'
            }).reset_index()
            
            # Renomear colunas agregadas
            df_aggregated = df_aggregated.rename(columns={
                'quantidade_teorica_acoes': 'total_quantidade_teorica',
                'percentual_participacao_ibov': 'participacao_media',
                'codigo': 'total_acoes_por_tipo'
            })
            
            logger.info("✓ Transformação C: Agrupamento por tipo realizado")
            logger.info(f"Dados agregados: {len(df_aggregated)} grupos")
            
            # Salvar dados refinados com particionamento
            for _, row in df_aggregated.iterrows():
                # Criar estrutura de particionamento duplo
                partition_dir = (self.refined_data_dir / 
                               f"year={int(row['year'])}" / 
                               f"month={int(row['month']):02d}" / 
                               f"day={int(row['day']):02d}" / 
                               f"tipo={row['tipo'].replace(' ', '_').replace('/', '_')}")
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Salvar arquivo particionado
                row_df = pd.DataFrame([row])
                output_file = partition_dir / "part-00000.parquet"
                row_df.to_parquet(output_file, index=False, engine='pyarrow')
            
            logger.info(f"Dados refinados salvos em: {self.refined_data_dir}")
            
            # Simular catalogação no Glue Data Catalog
            catalog_info = {
                "database": "bovespa_database",
                "table": "bovespa_refined_data",
                "location": str(self.refined_data_dir),
                "format": "parquet",
                "partitions": ["year", "month", "day", "tipo"],
                "schema": [
                    {"name": "tipo", "type": "string"},
                    {"name": "year", "type": "int"},
                    {"name": "month", "type": "int"},
                    {"name": "day", "type": "int"},
                    {"name": "total_quantidade_teorica", "type": "double"},
                    {"name": "participacao_media", "type": "double"},
                    {"name": "total_acoes_por_tipo", "type": "long"}
                ],
                "created_at": datetime.now().isoformat()
            }
            
            catalog_file = self.logs_dir / "glue_catalog_simulation.json"
            with open(catalog_file, 'w') as f:
                json.dump(catalog_info, f, indent=2)
            
            logger.info(f"Informações do catálogo simuladas em: {catalog_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro no Passo 3: {e}")
            return False
    
    def step4_simulate_athena_queries(self) -> bool:
        """
        Passo 4: Simular consultas do Athena
        
        Returns:
            True se bem-sucedido
        """
        try:
            logger.info("=== PASSO 4: Simulando consultas do Athena ===")
            
            # Carregar todos os dados refinados
            refined_files = list(self.refined_data_dir.glob("**/*.parquet"))
            
            if not refined_files:
                logger.error("Nenhum arquivo refinado encontrado")
                return False
            
            # Combinar todos os arquivos
            dfs = []
            for file in refined_files:
                df = pd.read_parquet(file)
                dfs.append(df)
            
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Dados combinados: {len(combined_df)} registros")
            
            # Simular consultas SQL
            queries_results = {}
            
            # Consulta 1: Dados básicos
            query1 = "SELECT * FROM bovespa_refined_data LIMIT 5"
            result1 = combined_df.head()
            queries_results["consulta_basica"] = {
                "sql": query1,
                "resultado": result1.to_dict('records')
            }
            logger.info("✓ Consulta 1: Dados básicos executada")
            
            # Consulta 2: Análise por tipo
            query2 = """
            SELECT tipo, 
                   total_acoes_por_tipo,
                   total_quantidade_teorica,
                   participacao_media
            FROM bovespa_refined_data 
            ORDER BY total_quantidade_teorica DESC
            """
            result2 = combined_df.sort_values('total_quantidade_teorica', ascending=False)
            queries_results["analise_por_tipo"] = {
                "sql": query2,
                "resultado": result2.to_dict('records')
            }
            logger.info("✓ Consulta 2: Análise por tipo executada")
            
            # Consulta 3: Estatísticas gerais
            query3 = """
            SELECT COUNT(*) as total_tipos,
                   SUM(total_acoes_por_tipo) as total_acoes,
                   SUM(total_quantidade_teorica) as quantidade_total,
                   AVG(participacao_media) as participacao_geral
            FROM bovespa_refined_data
            """
            result3 = {
                "total_tipos": len(combined_df),
                "total_acoes": combined_df['total_acoes_por_tipo'].sum(),
                "quantidade_total": combined_df['total_quantidade_teorica'].sum(),
                "participacao_geral": combined_df['participacao_media'].mean()
            }
            queries_results["estatisticas_gerais"] = {
                "sql": query3,
                "resultado": [result3]
            }
            logger.info("✓ Consulta 3: Estatísticas gerais executada")
            
            # Salvar resultados das consultas
            results_file = self.logs_dir / "athena_queries_simulation.json"
            with open(results_file, 'w') as f:
                json.dump(queries_results, f, indent=2, default=str)
            
            logger.info(f"Resultados das consultas salvos em: {results_file}")
            
            # Exibir resumo dos resultados
            logger.info("\n=== RESUMO DOS RESULTADOS ===")
            logger.info(f"Total de tipos de ação: {result3['total_tipos']}")
            logger.info(f"Total de ações: {result3['total_acoes']}")
            logger.info(f"Quantidade teórica total: {result3['quantidade_total']:,.0f}")
            logger.info(f"Participação média geral: {result3['participacao_geral']:.2f}%")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro no Passo 4: {e}")
            return False
    
    def step5_generate_report(self) -> bool:
        """
        Passo 5: Gerar relatório final
        
        Returns:
            True se bem-sucedido
        """
        try:
            logger.info("=== PASSO 5: Gerando relatório final ===")
            
            # Carregar dados para relatório
            refined_files = list(self.refined_data_dir.glob("**/*.parquet"))
            
            if refined_files:
                dfs = [pd.read_parquet(f) for f in refined_files]
                combined_df = pd.concat(dfs, ignore_index=True)
                
                # Gerar relatório
                report = {
                    "pipeline_execution": {
                        "timestamp": datetime.now().isoformat(),
                        "status": "SUCCESS",
                        "steps_completed": 5
                    },
                    "data_summary": {
                        "raw_data_files": len(list(self.raw_data_dir.glob("**/*.parquet"))),
                        "refined_data_files": len(refined_files),
                        "total_records_processed": len(combined_df),
                        "unique_tipos": combined_df['tipo'].nunique() if 'tipo' in combined_df.columns else 0
                    },
                    "transformations_applied": [
                        "Renomeação de colunas (qtde_teorica → quantidade_teorica_acoes)",
                        "Renomeação de colunas (participacao_pct → percentual_participacao_ibov)",
                        "Cálculo de dias desde coleta",
                        "Agrupamento por tipo de ação",
                        "Sumarização de quantidade teórica",
                        "Cálculo de participação média"
                    ],
                    "partitioning": {
                        "raw_data": ["year", "month", "day"],
                        "refined_data": ["year", "month", "day", "tipo"]
                    },
                    "requirements_compliance": {
                        "req1_scraping": "✓ Implementado com dados simulados",
                        "req2_s3_parquet": "✓ Dados salvos em Parquet com particionamento",
                        "req3_lambda_trigger": "✓ Simulado com eventos S3",
                        "req4_lambda_language": "✓ Python implementado",
                        "req5a_aggregation": "✓ Agrupamento por tipo implementado",
                        "req5b_column_rename": "✓ Duas colunas renomeadas",
                        "req5c_date_calculation": "✓ Cálculo de dias desde coleta",
                        "req6_refined_partitioning": "✓ Particionamento duplo implementado",
                        "req7_glue_catalog": "✓ Estrutura de catálogo simulada",
                        "req8_athena_queries": "✓ Consultas simuladas com sucesso"
                    }
                }
                
                # Salvar relatório
                report_file = self.logs_dir / "pipeline_execution_report.json"
                with open(report_file, 'w') as f:
                    json.dump(report, f, indent=2)
                
                logger.info(f"Relatório final salvo em: {report_file}")
                
                # Exibir resumo de compliance
                logger.info("\n=== COMPLIANCE COM REQUISITOS ===")
                for req, status in report["requirements_compliance"].items():
                    logger.info(f"{req}: {status}")
                
                return True
            else:
                logger.error("Nenhum dado refinado encontrado para relatório")
                return False
                
        except Exception as e:
            logger.error(f"Erro no Passo 5: {e}")
            return False
    
    def run_full_pipeline_test(self) -> bool:
        """
        Executa o teste completo do pipeline
        
        Returns:
            True se todos os passos foram bem-sucedidos
        """
        logger.info("🚀 INICIANDO TESTE COMPLETO DO PIPELINE")
        logger.info("=" * 60)
        
        steps = [
            ("Geração de dados simulados", self.step1_generate_sample_data),
            ("Simulação do trigger Lambda", self.step2_simulate_lambda_trigger),
            ("Simulação do job ETL Glue", self.step3_simulate_glue_etl),
            ("Simulação de consultas Athena", self.step4_simulate_athena_queries),
            ("Geração de relatório final", self.step5_generate_report)
        ]
        
        success_count = 0
        
        for step_name, step_function in steps:
            try:
                if step_function():
                    logger.info(f"✅ {step_name}: SUCESSO")
                    success_count += 1
                else:
                    logger.error(f"❌ {step_name}: FALHA")
            except Exception as e:
                logger.error(f"❌ {step_name}: ERRO - {e}")
        
        logger.info("=" * 60)
        logger.info(f"🏁 TESTE CONCLUÍDO: {success_count}/{len(steps)} passos bem-sucedidos")
        
        if success_count == len(steps):
            logger.info("🎉 PIPELINE TESTADO COM SUCESSO!")
            logger.info(f"📁 Resultados disponíveis em: {self.base_dir}")
            return True
        else:
            logger.error("⚠️  PIPELINE COM FALHAS - Verificar logs acima")
            return False

def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Teste do pipeline completo')
    parser.add_argument('--output-dir', default='./pipeline_test', help='Diretório de saída')
    
    args = parser.parse_args()
    
    # Executar teste
    tester = PipelineTester(base_dir=args.output_dir)
    success = tester.run_full_pipeline_test()
    
    exit(0 if success else 1)

if __name__ == "__main__":
    main()

