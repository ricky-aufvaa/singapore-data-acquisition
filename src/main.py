"""
Main ETL Pipeline Orchestrator for Singapore Company Database
Coordinates the entire data extraction, processing, and loading workflow
"""

import asyncio
import sys
import argparse
from datetime import datetime
from typing import Dict, Any, List
import uuid

from src.config import settings
from src.utils.logging_config import get_logger, LoggingContext
from src.database.connection import db_manager, DatabaseHealthCheck
from src.market_study import MarketStudyAnalyzer
from src.processors.llm_enricher import llm_enricher
from src.pipeline.extract_companies import CompanyExtractor
from src.pipeline.process_and_enrich import DataProcessor
from src.pipeline.load_to_database import DatabaseLoader

logger = get_logger(__name__, pipeline_id=str(uuid.uuid4()))


class PipelineOrchestrator:
    """Main pipeline orchestrator that coordinates all ETL operations"""
    
    def __init__(self):
        self.pipeline_id = str(uuid.uuid4())
        self.start_time = None
        self.stats = {
            'companies_extracted': 0,
            'companies_processed': 0,
            'companies_enriched': 0,
            'companies_loaded': 0,
            'errors': 0,
            'processing_time_seconds': 0
        }
        
    async def run_full_pipeline(self, skip_market_study: bool = False) -> Dict[str, Any]:
        """Run the complete ETL pipeline"""
        self.start_time = datetime.now()
        
        with LoggingContext(logger, "Full ETL Pipeline", pipeline_id=self.pipeline_id) as ctx:
            try:
                # Step 1: Market Study (optional)
                if not skip_market_study:
                    await self._run_market_study()
                
                # Step 2: Health Checks
                await self._run_health_checks()
                
                # Step 3: Data Extraction
                extracted_companies = await self._run_extraction()
                
                # Step 4: Data Processing & Enrichment
                processed_companies = await self._run_processing(extracted_companies)
                
                # Step 5: Database Loading
                await self._run_loading(processed_companies)
                
                # Step 6: Final Analytics
                analytics = await self._generate_analytics()
                
                # Calculate final stats
                self.stats['processing_time_seconds'] = (datetime.now() - self.start_time).total_seconds()
                
                logger.info("Pipeline completed successfully", extra={
                    'pipeline_stats': self.stats,
                    'analytics': analytics
                })
                
                return {
                    'status': 'success',
                    'pipeline_id': self.pipeline_id,
                    'stats': self.stats,
                    'analytics': analytics,
                    'completion_time': datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.error(f"Pipeline failed: {e}", extra={
                    'pipeline_id': self.pipeline_id,
                    'error_type': type(e).__name__,
                    'partial_stats': self.stats
                })
                raise
    
    async def _run_market_study(self):
        """Run market study and data source analysis"""
        with LoggingContext(logger, "Market Study", pipeline_id=self.pipeline_id):
            analyzer = MarketStudyAnalyzer()
            market_report = await analyzer.conduct_market_study()
            
            logger.info("Market study completed", extra={
                'sources_analyzed': market_report['total_sources_analyzed'],
                'estimated_coverage': market_report['estimated_total_coverage']['realistic_estimate']
            })
    
    async def _run_health_checks(self):
        """Run system health checks"""
        with LoggingContext(logger, "Health Checks", pipeline_id=self.pipeline_id):
            # Database health check
            db_health = DatabaseHealthCheck.check_database_health()
            if not db_health['connection_test']:
                raise RuntimeError("Database connection failed")
            
            # LLM health check
            llm_info = llm_enricher.get_model_info()
            if 'error' in llm_info:
                logger.warning(f"LLM not available: {llm_info['error']}")
            else:
                logger.info(f"LLM model ready: {llm_info['model_name']}")
            
            logger.info("Health checks passed")
    
    async def _run_extraction(self) -> List[Dict[str, Any]]:
        """Run data extraction from all sources"""
        with LoggingContext(logger, "Data Extraction", pipeline_id=self.pipeline_id):
            extractor = CompanyExtractor()
            
            # Extract from all configured sources
            companies = await extractor.extract_all_sources()
            
            self.stats['companies_extracted'] = len(companies)
            logger.info(f"Extracted {len(companies)} companies from all sources")
            
            return companies
    
    async def _run_processing(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run data processing and enrichment"""
        with LoggingContext(logger, "Data Processing", pipeline_id=self.pipeline_id):
            processor = DataProcessor()
            
            # Clean and normalize data
            cleaned_companies = await processor.clean_and_normalize(companies)
            self.stats['companies_processed'] = len(cleaned_companies)
            
            # Entity matching and deduplication
            deduplicated_companies = await processor.deduplicate_companies(cleaned_companies)
            
            # LLM enrichment
            enriched_companies = await processor.enrich_with_llm(deduplicated_companies)
            self.stats['companies_enriched'] = len(enriched_companies)
            
            logger.info(f"Processed and enriched {len(enriched_companies)} companies")
            
            return enriched_companies
    
    async def _run_loading(self, companies: List[Dict[str, Any]]):
        """Load processed data to database"""
        with LoggingContext(logger, "Database Loading", pipeline_id=self.pipeline_id):
            loader = DatabaseLoader()
            
            # Load companies to database
            loaded_count = await loader.load_companies(companies)
            self.stats['companies_loaded'] = loaded_count
            
            # Update analytics views
            await loader.refresh_analytics()
            
            logger.info(f"Loaded {loaded_count} companies to database")
    
    async def _generate_analytics(self) -> Dict[str, Any]:
        """Generate final analytics and insights"""
        with LoggingContext(logger, "Analytics Generation", pipeline_id=self.pipeline_id):
            # Get top industries
            industry_query = """
                SELECT industry, COUNT(*) as company_count 
                FROM companies 
                WHERE industry IS NOT NULL 
                GROUP BY industry 
                ORDER BY company_count DESC 
                LIMIT 10
            """
            
            industries = db_manager.execute_query(industry_query)
            
            # Get data quality metrics
            quality_query = """
                SELECT 
                    AVG(data_quality_score) as avg_quality_score,
                    COUNT(CASE WHEN website IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as website_coverage,
                    COUNT(CASE WHEN contact_email IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as email_coverage,
                    COUNT(CASE WHEN keywords IS NOT NULL AND array_length(keywords, 1) > 0 THEN 1 END) * 100.0 / COUNT(*) as keywords_coverage
                FROM companies
            """
            
            quality_metrics = db_manager.execute_query(quality_query)
            
            analytics = {
                'total_companies': self.stats['companies_loaded'],
                'top_industries': industries,
                'data_quality_metrics': quality_metrics[0] if quality_metrics else {},
                'pipeline_performance': {
                    'extraction_rate': self.stats['companies_extracted'] / max(1, self.stats['processing_time_seconds']),
                    'processing_rate': self.stats['companies_processed'] / max(1, self.stats['processing_time_seconds']),
                    'enrichment_rate': self.stats['companies_enriched'] / max(1, self.stats['processing_time_seconds'])
                }
            }
            
            return analytics


class PipelineRunner:
    """Command-line interface for running the pipeline"""
    
    @staticmethod
    def create_argument_parser():
        """Create command-line argument parser"""
        parser = argparse.ArgumentParser(
            description="Singapore Company Database ETL Pipeline",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  python src/main.py --full                    # Run complete pipeline
  python src/main.py --extract-only           # Extract data only
  python src/main.py --market-study           # Run market study only
  python src/main.py --health-check           # Run health checks only
  python src/main.py --full --skip-market-study  # Skip market study
            """
        )
        
        # Pipeline modes
        parser.add_argument('--full', action='store_true',
                          help='Run complete ETL pipeline')
        parser.add_argument('--extract-only', action='store_true',
                          help='Run extraction phase only')
        parser.add_argument('--process-only', action='store_true',
                          help='Run processing phase only')
        parser.add_argument('--market-study', action='store_true',
                          help='Run market study only')
        parser.add_argument('--health-check', action='store_true',
                          help='Run health checks only')
        
        # Pipeline options
        parser.add_argument('--skip-market-study', action='store_true',
                          help='Skip market study in full pipeline')
        parser.add_argument('--batch-size', type=int, default=1000,
                          help='Processing batch size')
        parser.add_argument('--max-companies', type=int,
                          help='Maximum number of companies to process')
        
        # Data source options
        parser.add_argument('--sources', nargs='+',
                          help='Specific data sources to extract from')
        parser.add_argument('--skip-llm', action='store_true',
                          help='Skip LLM enrichment')
        
        # Output options
        parser.add_argument('--output-format', choices=['json', 'csv', 'both'],
                          default='json', help='Output format for results')
        parser.add_argument('--output-dir', default='data/output',
                          help='Output directory for results')
        
        return parser
    
    @staticmethod
    async def run_market_study():
        """Run market study only"""
        analyzer = MarketStudyAnalyzer()
        report = await analyzer.conduct_market_study()
        
        print("\n" + "="*60)
        print("MARKET STUDY COMPLETED")
        print("="*60)
        print(f"Sources Analyzed: {report['total_sources_analyzed']}")
        print(f"Estimated Coverage: {report['estimated_total_coverage']['realistic_estimate']:,} companies")
        print(f"Report saved to: data/reports/market_study_report.json")
        print("="*60)
    
    @staticmethod
    async def run_health_check():
        """Run health checks only"""
        print("\n" + "="*60)
        print("SYSTEM HEALTH CHECK")
        print("="*60)
        
        # Database health
        db_health = DatabaseHealthCheck.check_database_health()
        db_status = "✓ PASS" if db_health['connection_test'] else "✗ FAIL"
        print(f"Database Connection: {db_status}")
        
        if db_health['connection_test']:
            print(f"Database Size: {db_health['database_size'].get('size', 'Unknown')}")
            print(f"Tables Found: {len(db_health['table_stats'])}")
        
        # LLM health
        llm_info = llm_enricher.get_model_info()
        llm_status = "✓ PASS" if 'error' not in llm_info else "✗ FAIL"
        print(f"LLM Model: {llm_status}")
        
        if 'error' not in llm_info:
            print(f"Model Name: {llm_info['model_name']}")
        
        print("="*60)
    
    @staticmethod
    async def run_extraction_only(sources=None, max_companies=None):
        """Run extraction phase only"""
        extractor = CompanyExtractor()
        
        if sources:
            companies = []
            for source in sources:
                source_companies = await extractor.extract_from_source(source)
                companies.extend(source_companies)
        else:
            companies = await extractor.extract_all_sources()
        
        if max_companies:
            companies = companies[:max_companies]
        
        print(f"\nExtracted {len(companies)} companies")
        
        # Save to file
        import json
        import os
        os.makedirs('data/output', exist_ok=True)
        
        with open('data/output/extracted_companies.json', 'w') as f:
            json.dump(companies, f, indent=2, default=str)
        
        print("Results saved to: data/output/extracted_companies.json")


async def main():
    """Main entry point"""
    parser = PipelineRunner.create_argument_parser()
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    try:
        # Initialize database
        if not settings.testing:
            db_manager.initialize()
        
        # Route to appropriate function
        if args.market_study:
            await PipelineRunner.run_market_study()
        
        elif args.health_check:
            await PipelineRunner.run_health_check()
        
        elif args.extract_only:
            await PipelineRunner.run_extraction_only(
                sources=args.sources,
                max_companies=args.max_companies
            )
        
        elif args.full:
            orchestrator = PipelineOrchestrator()
            result = await orchestrator.run_full_pipeline(
                skip_market_study=args.skip_market_study
            )
            
            print("\n" + "="*60)
            print("PIPELINE COMPLETED SUCCESSFULLY")
            print("="*60)
            print(f"Pipeline ID: {result['pipeline_id']}")
            print(f"Companies Processed: {result['stats']['companies_loaded']:,}")
            print(f"Processing Time: {result['stats']['processing_time_seconds']:.1f} seconds")
            print(f"Data Quality Score: {result['analytics']['data_quality_metrics'].get('avg_quality_score', 0):.2f}")
            print("="*60)
            
            # Save results
            import json
            import os
            os.makedirs(args.output_dir, exist_ok=True)
            
            with open(f"{args.output_dir}/pipeline_result.json", 'w') as f:
                json.dump(result, f, indent=2, default=str)
            
            print(f"Results saved to: {args.output_dir}/pipeline_result.json")
        
        else:
            print("Please specify a pipeline mode. Use --help for options.")
            parser.print_help()
    
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        print("\nPipeline interrupted by user")
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        print(f"\nPipeline failed: {e}")
        sys.exit(1)
    
    finally:
        # Cleanup
        if hasattr(llm_enricher, 'close'):
            llm_enricher.close()
        
        if hasattr(db_manager, 'close_connections'):
            db_manager.close_connections()


if __name__ == "__main__":
    # Set up event loop policy for Windows compatibility
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # Run the main function
    asyncio.run(main())
