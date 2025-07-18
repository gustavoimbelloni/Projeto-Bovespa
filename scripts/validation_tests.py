#!/usr/bin/env python3
"""
Script de validação e testes específicos para o Tech Challenge
Valida cada requisito individualmente e gera relatório detalhado
"""

import pandas as pd
import os
import json
import logging
from datetime import datetime, date
from pathlib import Path
import unittest
from typing import Dict, List, Any

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TechChallengeValidator:
    """Classe para validar todos os requisitos do Tech Challenge"""
    
    def __init__(self, pipeline_dir: str = "./pipeline_test"):
        """
        Inicializa o validador
        
        Args:
            pipeline_dir: Diretório com os resultados do pipeline
        """
        self.pipeline_dir = Path(pipeline_dir)
        self.raw_data_dir = self.pipeline_dir / "raw-data"
        self.refined_data_dir = self.pipeline_dir / "refined-data"
        self.logs_dir = self.pipeline_dir / "logs"
        
        self.validation_results = {}
    
    def validate_requirement_1(self) -> Dict[str, Any]:
        """
        Valida Requisito 1: Scrap de dados do site da B3
        
        Returns:
            Resultado da validação
        """
        logger.info("🔍 Validando Requisito 1: Scrap de dados da B3")
        
        result = {
            "requirement": "Req 1 - Scrap de dados do site da B3",
            "status": "PASS",
            "details": [],
            "issues": []
        }
        
        try:
            # Verificar se existe script de scrap
            script_path = Path("scripts/b3_scraper.py")
            if script_path.exists():
                result["details"].append("✓ Script de scrap implementado")
                
                # Verificar se o script contém as funcionalidades necessárias
                with open(script_path, 'r') as f:
                    script_content = f.read()
                
                required_features = [
                    ("requests", "Biblioteca requests para HTTP"),
                    ("BeautifulSoup", "Parser HTML BeautifulSoup"),
                    ("parquet", "Suporte a formato Parquet"),
                    ("boto3", "Integração com AWS S3")
                ]
                
                for feature, description in required_features:
                    if feature in script_content:
                        result["details"].append(f"✓ {description} presente")
                    else:
                        result["issues"].append(f"⚠ {description} não encontrado")
                
            else:
                result["status"] = "FAIL"
                result["issues"].append("❌ Script de scrap não encontrado")
            
            # Verificar se dados foram coletados
            raw_files = list(self.raw_data_dir.glob("**/*.parquet"))
            if raw_files:
                result["details"].append(f"✓ {len(raw_files)} arquivo(s) de dados brutos encontrado(s)")
                
                # Verificar estrutura dos dados
                df = pd.read_parquet(raw_files[0])
                expected_columns = ['codigo', 'acao', 'tipo', 'qtde_teorica', 'participacao_pct']
                
                for col in expected_columns:
                    if col in df.columns:
                        result["details"].append(f"✓ Coluna '{col}' presente nos dados")
                    else:
                        result["issues"].append(f"⚠ Coluna '{col}' ausente nos dados")
                        
            else:
                result["issues"].append("⚠ Nenhum arquivo de dados brutos encontrado")
                
        except Exception as e:
            result["status"] = "ERROR"
            result["issues"].append(f"❌ Erro na validação: {str(e)}")
        
        return result
    
    def validate_requirement_2(self) -> Dict[str, Any]:
        """
        Valida Requisito 2: Dados brutos no S3 em formato Parquet com partição diária
        
        Returns:
            Resultado da validação
        """
        logger.info("🔍 Validando Requisito 2: S3 Parquet com partição diária")
        
        result = {
            "requirement": "Req 2 - S3 Parquet com partição diária",
            "status": "PASS",
            "details": [],
            "issues": []
        }
        
        try:
            # Verificar estrutura de particionamento
            expected_structure = ["year=", "month=", "day="]
            
            for root, dirs, files in os.walk(self.raw_data_dir):
                path_parts = Path(root).parts
                
                # Verificar se contém particionamento por data
                has_year = any("year=" in part for part in path_parts)
                has_month = any("month=" in part for part in path_parts)
                has_day = any("day=" in part for part in path_parts)
                
                if has_year and has_month and has_day:
                    result["details"].append("✓ Particionamento por year/month/day implementado")
                    break
            else:
                result["issues"].append("⚠ Particionamento por data não encontrado")
            
            # Verificar arquivos Parquet
            parquet_files = list(self.raw_data_dir.glob("**/*.parquet"))
            if parquet_files:
                result["details"].append(f"✓ {len(parquet_files)} arquivo(s) Parquet encontrado(s)")
                
                # Verificar se é possível ler os arquivos
                for file in parquet_files:
                    try:
                        df = pd.read_parquet(file)
                        result["details"].append(f"✓ Arquivo {file.name} legível ({len(df)} registros)")
                    except Exception as e:
                        result["issues"].append(f"❌ Erro ao ler {file.name}: {str(e)}")
                        
            else:
                result["status"] = "FAIL"
                result["issues"].append("❌ Nenhum arquivo Parquet encontrado")
                
        except Exception as e:
            result["status"] = "ERROR"
            result["issues"].append(f"❌ Erro na validação: {str(e)}")
        
        return result
    
    def validate_requirement_3_4(self) -> Dict[str, Any]:
        """
        Valida Requisitos 3 e 4: Lambda trigger e linguagem
        
        Returns:
            Resultado da validação
        """
        logger.info("🔍 Validando Requisitos 3 e 4: Lambda trigger")
        
        result = {
            "requirement": "Req 3-4 - Lambda trigger para Glue",
            "status": "PASS",
            "details": [],
            "issues": []
        }
        
        try:
            # Verificar se existe função Lambda
            lambda_path = Path("lambda/trigger_glue_job.py")
            if lambda_path.exists():
                result["details"].append("✓ Função Lambda implementada")
                
                with open(lambda_path, 'r') as f:
                    lambda_content = f.read()
                
                # Verificar funcionalidades da Lambda
                required_features = [
                    ("boto3", "Cliente AWS SDK"),
                    ("glue", "Integração com AWS Glue"),
                    ("start_job_run", "Inicialização de job Glue"),
                    ("Records", "Processamento de eventos S3")
                ]
                
                for feature, description in required_features:
                    if feature in lambda_content:
                        result["details"].append(f"✓ {description} implementado")
                    else:
                        result["issues"].append(f"⚠ {description} não encontrado")
                
                # Verificar se é Python (Requisito 4)
                if "python" in lambda_content.lower() or lambda_path.suffix == ".py":
                    result["details"].append("✓ Linguagem Python utilizada")
                
            else:
                result["status"] = "FAIL"
                result["issues"].append("❌ Função Lambda não encontrada")
            
            # Verificar simulação de evento S3
            event_file = self.logs_dir / "s3_event_simulation.json"
            if event_file.exists():
                result["details"].append("✓ Evento S3 simulado")
                
                with open(event_file, 'r') as f:
                    event_data = json.load(f)
                
                if "Records" in event_data and event_data["Records"]:
                    result["details"].append("✓ Estrutura de evento S3 válida")
                else:
                    result["issues"].append("⚠ Estrutura de evento S3 inválida")
                    
            else:
                result["issues"].append("⚠ Simulação de evento S3 não encontrada")
                
        except Exception as e:
            result["status"] = "ERROR"
            result["issues"].append(f"❌ Erro na validação: {str(e)}")
        
        return result
    
    def validate_requirement_5(self) -> Dict[str, Any]:
        """
        Valida Requisito 5: Job Glue visual com transformações
        
        Returns:
            Resultado da validação
        """
        logger.info("🔍 Validando Requisito 5: Job Glue com transformações")
        
        result = {
            "requirement": "Req 5 - Job Glue visual com transformações",
            "status": "PASS",
            "details": [],
            "issues": []
        }
        
        try:
            # Verificar configuração do job Glue
            glue_config = Path("glue-jobs/glue_job_config.json")
            if glue_config.exists():
                result["details"].append("✓ Configuração do job Glue encontrada")
                
                with open(glue_config, 'r') as f:
                    config = json.load(f)
                
                # Verificar modo visual
                if config.get("jobMode") == "VISUAL":
                    result["details"].append("✓ Job configurado em modo visual")
                else:
                    result["issues"].append("⚠ Job não está em modo visual")
                
            else:
                result["issues"].append("⚠ Configuração do job Glue não encontrada")
            
            # Verificar dados refinados para validar transformações
            refined_files = list(self.refined_data_dir.glob("**/*.parquet"))
            if refined_files:
                result["details"].append(f"✓ {len(refined_files)} arquivo(s) refinado(s) encontrado(s)")
                
                # Carregar dados para verificar transformações
                dfs = [pd.read_parquet(f) for f in refined_files]
                combined_df = pd.concat(dfs, ignore_index=True)
                
                # Verificar Transformação A: Agrupamento/Sumarização
                if 'total_quantidade_teorica' in combined_df.columns:
                    result["details"].append("✓ Transformação A: Sumarização implementada")
                else:
                    result["issues"].append("❌ Transformação A: Sumarização não encontrada")
                
                # Verificar Transformação B: Renomeação de colunas
                renamed_columns = ['quantidade_teorica_acoes', 'percentual_participacao_ibov']
                found_renamed = [col for col in renamed_columns if col in combined_df.columns]
                
                if len(found_renamed) >= 2:
                    result["details"].append(f"✓ Transformação B: {len(found_renamed)} colunas renomeadas")
                else:
                    result["issues"].append(f"❌ Transformação B: Apenas {len(found_renamed)} colunas renomeadas (necessário 2)")
                
                # Verificar Transformação C: Cálculo com data
                date_columns = [col for col in combined_df.columns if 'dia' in col.lower() or 'date' in col.lower()]
                if date_columns:
                    result["details"].append("✓ Transformação C: Cálculo com data implementado")
                else:
                    result["issues"].append("❌ Transformação C: Cálculo com data não encontrado")
                
            else:
                result["status"] = "FAIL"
                result["issues"].append("❌ Nenhum arquivo refinado encontrado")
                
        except Exception as e:
            result["status"] = "ERROR"
            result["issues"].append(f"❌ Erro na validação: {str(e)}")
        
        return result
    
    def validate_requirement_6(self) -> Dict[str, Any]:
        """
        Valida Requisito 6: Dados refinados com particionamento
        
        Returns:
            Resultado da validação
        """
        logger.info("🔍 Validando Requisito 6: Dados refinados particionados")
        
        result = {
            "requirement": "Req 6 - Dados refinados particionados",
            "status": "PASS",
            "details": [],
            "issues": []
        }
        
        try:
            # Verificar estrutura de particionamento refinado
            refined_structure_found = False
            
            for root, dirs, files in os.walk(self.refined_data_dir):
                path_parts = Path(root).parts
                
                # Verificar particionamento por data e ação
                has_year = any("year=" in part for part in path_parts)
                has_month = any("month=" in part for part in path_parts)
                has_day = any("day=" in part for part in path_parts)
                has_action = any("tipo=" in part for part in path_parts)
                
                if has_year and has_month and has_day and has_action:
                    refined_structure_found = True
                    result["details"].append("✓ Particionamento por data e tipo de ação implementado")
                    break
            
            if not refined_structure_found:
                result["issues"].append("❌ Particionamento duplo (data + ação) não encontrado")
            
            # Verificar pasta refined
            if "refined" in str(self.refined_data_dir):
                result["details"].append("✓ Pasta 'refined' utilizada")
            else:
                result["issues"].append("⚠ Pasta 'refined' não identificada")
            
            # Verificar formato Parquet nos dados refinados
            refined_parquet = list(self.refined_data_dir.glob("**/*.parquet"))
            if refined_parquet:
                result["details"].append(f"✓ {len(refined_parquet)} arquivo(s) Parquet refinado(s)")
            else:
                result["status"] = "FAIL"
                result["issues"].append("❌ Nenhum arquivo Parquet refinado encontrado")
                
        except Exception as e:
            result["status"] = "ERROR"
            result["issues"].append(f"❌ Erro na validação: {str(e)}")
        
        return result
    
    def validate_requirement_7(self) -> Dict[str, Any]:
        """
        Valida Requisito 7: Catalogação automática no Glue Catalog
        
        Returns:
            Resultado da validação
        """
        logger.info("🔍 Validando Requisito 7: Glue Data Catalog")
        
        result = {
            "requirement": "Req 7 - Glue Data Catalog",
            "status": "PASS",
            "details": [],
            "issues": []
        }
        
        try:
            # Verificar simulação do catálogo
            catalog_file = self.logs_dir / "glue_catalog_simulation.json"
            if catalog_file.exists():
                result["details"].append("✓ Configuração do Data Catalog simulada")
                
                with open(catalog_file, 'r') as f:
                    catalog_data = json.load(f)
                
                # Verificar database default
                if catalog_data.get("database") == "default" or "default" in str(catalog_data.get("database", "")):
                    result["details"].append("✓ Database 'default' configurado")
                else:
                    result["issues"].append("⚠ Database 'default' não configurado")
                
                # Verificar tabela
                if "table" in catalog_data:
                    result["details"].append(f"✓ Tabela '{catalog_data.get('table')}' configurada")
                else:
                    result["issues"].append("⚠ Configuração de tabela não encontrada")
                
                # Verificar schema
                if "schema" in catalog_data:
                    result["details"].append("✓ Schema da tabela definido")
                else:
                    result["issues"].append("⚠ Schema da tabela não definido")
                
            else:
                result["issues"].append("⚠ Configuração do Data Catalog não encontrada")
            
            # Verificar configuração no job Glue
            glue_config = Path("glue-jobs/glue_job_config.json")
            if glue_config.exists():
                with open(glue_config, 'r') as f:
                    config = json.load(f)
                
                # Procurar por configuração de catalogação
                dag_str = json.dumps(config.get("dag", {}))
                if "enableUpdateCatalog" in dag_str or "UPDATE_IN_DATABASE" in dag_str:
                    result["details"].append("✓ Catalogação automática configurada no job Glue")
                else:
                    result["issues"].append("⚠ Catalogação automática não configurada")
                    
        except Exception as e:
            result["status"] = "ERROR"
            result["issues"].append(f"❌ Erro na validação: {str(e)}")
        
        return result
    
    def validate_requirement_8(self) -> Dict[str, Any]:
        """
        Valida Requisito 8: Dados disponíveis no Athena
        
        Returns:
            Resultado da validação
        """
        logger.info("🔍 Validando Requisito 8: Disponibilidade no Athena")
        
        result = {
            "requirement": "Req 8 - Dados disponíveis no Athena",
            "status": "PASS",
            "details": [],
            "issues": []
        }
        
        try:
            # Verificar simulação de consultas Athena
            athena_file = self.logs_dir / "athena_queries_simulation.json"
            if athena_file.exists():
                result["details"].append("✓ Consultas Athena simuladas")
                
                with open(athena_file, 'r') as f:
                    queries_data = json.load(f)
                
                # Verificar tipos de consulta
                query_types = list(queries_data.keys())
                result["details"].append(f"✓ {len(query_types)} tipo(s) de consulta testado(s)")
                
                # Verificar se há resultados
                for query_type, query_info in queries_data.items():
                    if "resultado" in query_info and query_info["resultado"]:
                        result["details"].append(f"✓ Consulta '{query_type}' retornou dados")
                    else:
                        result["issues"].append(f"⚠ Consulta '{query_type}' sem resultados")
                
            else:
                result["issues"].append("⚠ Simulação de consultas Athena não encontrada")
            
            # Verificar se dados refinados são legíveis (simulando Athena)
            refined_files = list(self.refined_data_dir.glob("**/*.parquet"))
            readable_files = 0
            
            for file in refined_files:
                try:
                    df = pd.read_parquet(file)
                    if len(df) > 0:
                        readable_files += 1
                except:
                    pass
            
            if readable_files > 0:
                result["details"].append(f"✓ {readable_files} arquivo(s) legível(is) para consulta")
            else:
                result["status"] = "FAIL"
                result["issues"].append("❌ Nenhum arquivo legível encontrado")
                
        except Exception as e:
            result["status"] = "ERROR"
            result["issues"].append(f"❌ Erro na validação: {str(e)}")
        
        return result
    
    def run_full_validation(self) -> Dict[str, Any]:
        """
        Executa validação completa de todos os requisitos
        
        Returns:
            Relatório completo de validação
        """
        logger.info("🔍 INICIANDO VALIDAÇÃO COMPLETA DOS REQUISITOS")
        logger.info("=" * 60)
        
        validators = [
            self.validate_requirement_1,
            self.validate_requirement_2,
            self.validate_requirement_3_4,
            self.validate_requirement_5,
            self.validate_requirement_6,
            self.validate_requirement_7,
            self.validate_requirement_8
        ]
        
        validation_results = []
        passed_count = 0
        
        for validator in validators:
            try:
                result = validator()
                validation_results.append(result)
                
                # Log resultado
                status_emoji = "✅" if result["status"] == "PASS" else "❌" if result["status"] == "FAIL" else "⚠️"
                logger.info(f"{status_emoji} {result['requirement']}: {result['status']}")
                
                if result["status"] == "PASS":
                    passed_count += 1
                
                # Log detalhes
                for detail in result["details"]:
                    logger.info(f"    {detail}")
                
                # Log issues
                for issue in result["issues"]:
                    logger.warning(f"    {issue}")
                
            except Exception as e:
                logger.error(f"❌ Erro na validação: {str(e)}")
        
        # Relatório final
        total_requirements = len(validation_results)
        success_rate = (passed_count / total_requirements) * 100 if total_requirements > 0 else 0
        
        final_report = {
            "validation_summary": {
                "timestamp": datetime.now().isoformat(),
                "total_requirements": total_requirements,
                "passed": passed_count,
                "failed": total_requirements - passed_count,
                "success_rate": round(success_rate, 2)
            },
            "detailed_results": validation_results,
            "overall_status": "PASS" if passed_count == total_requirements else "PARTIAL" if passed_count > 0 else "FAIL"
        }
        
        # Salvar relatório
        report_file = self.logs_dir / "validation_report.json"
        with open(report_file, 'w') as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)
        
        logger.info("=" * 60)
        logger.info(f"🏁 VALIDAÇÃO CONCLUÍDA: {passed_count}/{total_requirements} requisitos aprovados")
        logger.info(f"📊 Taxa de sucesso: {success_rate:.1f}%")
        logger.info(f"📄 Relatório salvo em: {report_file}")
        
        return final_report

def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validação dos requisitos do Tech Challenge')
    parser.add_argument('--pipeline-dir', default='./pipeline_test', help='Diretório do pipeline')
    
    args = parser.parse_args()
    
    # Executar validação
    validator = TechChallengeValidator(pipeline_dir=args.pipeline_dir)
    report = validator.run_full_validation()
    
    # Exit code baseado no resultado
    if report["overall_status"] == "PASS":
        exit(0)
    elif report["overall_status"] == "PARTIAL":
        exit(1)
    else:
        exit(2)

if __name__ == "__main__":
    main()

