"""
Database Loader for Singapore Company Database
Handles loading processed company data into PostgreSQL database
"""

import asyncio
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
import uuid

from src.config import settings
from src.utils.logging_config import get_logger, LoggingContext
from src.database.connection import db_manager, batch_insert, upsert_records

logger = get_logger(__name__)


class DatabaseLoader:
    """Main class for loading company data into database"""
    
    def __init__(self):
        self.loaded_companies = 0
        self.failed_companies = 0
        self.updated_companies = 0
        self.load_errors = []
        
    async def load_companies(self, companies: List[Dict[str, Any]]) -> int:
        """Load companies into database"""
        with LoggingContext(logger, "Database Loading"):
            # Prepare data for database insertion
            prepared_companies = self._prepare_companies_for_db(companies)
            
            # Load main company records
            loaded_count = await self._load_main_companies(prepared_companies)
            
            # Load related data
            await self._load_social_media_data(companies)
            await self._load_financial_data(companies)
            await self._load_location_data(companies)
            await self._load_field_sources(companies)
            
            # Update data quality scores
            await self._update_data_quality_scores()
            
            logger.info(f"Database loading complete: {loaded_count} companies loaded")
            
        return loaded_count
    
    def _prepare_companies_for_db(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare company data for database insertion"""
        prepared = []
        
        for company in companies:
            try:
                db_record = self._map_company_to_db_schema(company)
                if db_record:
                    prepared.append(db_record)
            except Exception as e:
                logger.error(f"Error preparing company {company.get('company_name', 'Unknown')}: {e}")
                self.load_errors.append({
                    'company': company.get('company_name', 'Unknown'),
                    'error': str(e),
                    'stage': 'preparation'
                })
        
        return prepared
    
    def _map_company_to_db_schema(self, company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Map company data to database schema"""
        # Generate UEN if missing
        uen = company.get('uen')
        if not uen:
            # Generate a temporary UEN for companies without one
            year = company.get('founding_year', 2020)
            random_num = abs(hash(company.get('company_name', ''))) % 1000000
            uen = f"{year}{random_num:06d}T"  # T for temporary
        
        # Map to database schema
        db_record = {
            'uen': uen,
            'company_name': company.get('company_name'),
            'company_name_normalized': company.get('company_name_normalized'),
            'website': company.get('website'),
            'hq_country': company.get('hq_country', 'Singapore'),
            'no_of_locations_in_singapore': company.get('no_of_locations_in_singapore', 1),
            'industry': company.get('industry'),
            'number_of_employees': company.get('number_of_employees'),
            'company_size': company.get('company_size'),
            'is_it_delisted': company.get('is_it_delisted', False),
            'stock_exchange_code': company.get('stock_exchange_code'),
            'revenue': company.get('revenue'),
            'founding_year': company.get('founding_year'),
            'contact_email': company.get('contact_email'),
            'contact_phone': company.get('contact_phone'),
            'keywords': company.get('keywords', []),
            'products_offered': company.get('products_offered', []),
            'services_offered': company.get('services_offered', []),
            'data_quality_score': company.get('data_quality_score', 0.0),
            'last_updated': datetime.now(),
            'created_at': datetime.now()
        }
        
        # Remove None values
        db_record = {k: v for k, v in db_record.items() if v is not None}
        
        return db_record
    
    async def _load_main_companies(self, companies: List[Dict[str, Any]]) -> int:
        """Load main company records"""
        if not companies:
            return 0
        
        try:
            # Use upsert to handle duplicates
            loaded_count = upsert_records(
                table='companies',
                data=companies,
                conflict_columns=['uen'],
                update_columns=[
                    'company_name', 'website', 'industry', 'number_of_employees',
                    'company_size', 'revenue', 'contact_email', 'contact_phone',
                    'keywords', 'products_offered', 'services_offered',
                    'data_quality_score', 'last_updated'
                ]
            )
            
            self.loaded_companies = loaded_count
            return loaded_count
            
        except Exception as e:
            logger.error(f"Error loading main companies: {e}")
            self.failed_companies = len(companies)
            raise
    
    async def _load_social_media_data(self, companies: List[Dict[str, Any]]):
        """Load social media data into separate table"""
        social_media_records = []
        
        for company in companies:
            uen = company.get('uen')
            if not uen:
                continue
            
            social_data = {}
            for field in ['linkedin', 'facebook', 'instagram', 'twitter', 'youtube', 'tiktok']:
                if company.get(field):
                    social_data[field] = company[field]
            
            if social_data:
                social_record = {
                    'id': str(uuid.uuid4()),
                    'uen': uen,
                    **social_data,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                social_media_records.append(social_record)
        
        if social_media_records:
            try:
                upsert_records(
                    table='company_social_media',
                    data=social_media_records,
                    conflict_columns=['uen'],
                    update_columns=['linkedin', 'facebook', 'instagram', 'twitter', 'youtube', 'tiktok', 'updated_at']
                )
                logger.info(f"Loaded {len(social_media_records)} social media records")
            except Exception as e:
                logger.error(f"Error loading social media data: {e}")
    
    async def _load_financial_data(self, companies: List[Dict[str, Any]]):
        """Load financial data into separate table"""
        financial_records = []
        
        for company in companies:
            uen = company.get('uen')
            if not uen:
                continue
            
            # Check if company has financial data
            has_financial_data = any(company.get(field) for field in [
                'revenue', 'stock_exchange_code', 'is_it_delisted'
            ])
            
            if has_financial_data:
                financial_record = {
                    'id': str(uuid.uuid4()),
                    'uen': uen,
                    'annual_revenue': company.get('revenue'),
                    'revenue_currency': 'SGD',
                    'revenue_year': datetime.now().year,
                    'is_profitable': None,  # Would need additional data
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                # Remove None values
                financial_record = {k: v for k, v in financial_record.items() if v is not None}
                financial_records.append(financial_record)
        
        if financial_records:
            try:
                upsert_records(
                    table='company_financials',
                    data=financial_records,
                    conflict_columns=['uen'],
                    update_columns=['annual_revenue', 'revenue_year', 'updated_at']
                )
                logger.info(f"Loaded {len(financial_records)} financial records")
            except Exception as e:
                logger.error(f"Error loading financial data: {e}")
    
    async def _load_location_data(self, companies: List[Dict[str, Any]]):
        """Load location data into separate table"""
        location_records = []
        
        for company in companies:
            uen = company.get('uen')
            if not uen:
                continue
            
            # Create a basic location record for Singapore companies
            location_record = {
                'id': str(uuid.uuid4()),
                'uen': uen,
                'location_type': 'headquarters',
                'city': 'Singapore',
                'country': 'Singapore',
                'is_primary': True,
                'created_at': datetime.now()
            }
            location_records.append(location_record)
        
        if location_records:
            try:
                # Use batch insert for location data (assuming no duplicates)
                batch_insert(
                    table='company_locations',
                    data=location_records,
                    on_conflict='ON CONFLICT (uen, location_type) DO NOTHING'
                )
                logger.info(f"Loaded {len(location_records)} location records")
            except Exception as e:
                logger.error(f"Error loading location data: {e}")
    
    async def _load_field_sources(self, companies: List[Dict[str, Any]]):
        """Load field source tracking data"""
        source_records = []
        
        # Get data source IDs
        source_query = "SELECT id, source_name FROM data_sources"
        data_sources = db_manager.execute_query(source_query)
        source_map = {source['source_name']: source['id'] for source in data_sources}
        
        for company in companies:
            uen = company.get('uen')
            if not uen:
                continue
            
            source_of_data = company.get('source_of_data', 'Unknown')
            
            # Map common fields to their sources
            field_mappings = {
                'company_name': source_of_data,
                'website': source_of_data,
                'industry': source_of_data,
                'contact_email': source_of_data,
                'contact_phone': source_of_data,
                'keywords': 'Llama 3 LLM' if company.get('keywords') else source_of_data
            }
            
            for field_name, source_name in field_mappings.items():
                if company.get(field_name):
                    # Find matching source ID
                    source_id = None
                    for db_source_name, db_source_id in source_map.items():
                        if db_source_name.lower() in source_name.lower():
                            source_id = db_source_id
                            break
                    
                    if source_id:
                        source_record = {
                            'id': str(uuid.uuid4()),
                            'uen': uen,
                            'field_name': field_name,
                            'source_id': source_id,
                            'confidence_score': 0.8,  # Default confidence
                            'extracted_at': datetime.now()
                        }
                        source_records.append(source_record)
        
        if source_records:
            try:
                batch_insert(
                    table='company_field_sources',
                    data=source_records,
                    on_conflict='ON CONFLICT (uen, field_name, source_id) DO NOTHING'
                )
                logger.info(f"Loaded {len(source_records)} field source records")
            except Exception as e:
                logger.error(f"Error loading field source data: {e}")
    
    async def _update_data_quality_scores(self):
        """Update data quality scores using database function"""
        try:
            query = "SELECT update_data_quality_scores()"
            result = db_manager.execute_query(query)
            
            if result:
                updated_count = result[0].get('update_data_quality_scores', 0)
                logger.info(f"Updated data quality scores for {updated_count} companies")
            
        except Exception as e:
            logger.error(f"Error updating data quality scores: {e}")
    
    async def refresh_analytics(self):
        """Refresh materialized views and analytics"""
        try:
            # Refresh materialized view
            refresh_query = "REFRESH MATERIALIZED VIEW company_analytics"
            db_manager.execute_query(refresh_query)
            
            # Update table statistics
            analyze_query = "ANALYZE companies, company_social_media, company_financials, company_locations"
            db_manager.execute_query(analyze_query)
            
            logger.info("Analytics views refreshed successfully")
            
        except Exception as e:
            logger.error(f"Error refreshing analytics: {e}")
    
    def get_load_stats(self) -> Dict[str, Any]:
        """Get loading statistics"""
        return {
            'loaded_companies': self.loaded_companies,
            'failed_companies': self.failed_companies,
            'updated_companies': self.updated_companies,
            'load_errors': len(self.load_errors),
            'success_rate': (self.loaded_companies / max(1, self.loaded_companies + self.failed_companies)) * 100
        }
    
    async def validate_loaded_data(self) -> Dict[str, Any]:
        """Validate loaded data and return statistics"""
        validation_results = {}
        
        try:
            # Count total companies
            count_query = "SELECT COUNT(*) as total FROM companies"
            total_result = db_manager.execute_query(count_query)
            validation_results['total_companies'] = total_result[0]['total'] if total_result else 0
            
            # Count companies with key fields
            coverage_query = """
                SELECT 
                    COUNT(CASE WHEN website IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as website_coverage,
                    COUNT(CASE WHEN contact_email IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as email_coverage,
                    COUNT(CASE WHEN contact_phone IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as phone_coverage,
                    COUNT(CASE WHEN linkedin IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as linkedin_coverage,
                    COUNT(CASE WHEN keywords IS NOT NULL AND array_length(keywords, 1) > 0 THEN 1 END) * 100.0 / COUNT(*) as keywords_coverage,
                    AVG(data_quality_score) as avg_quality_score
                FROM companies c
                LEFT JOIN company_social_media sm ON c.uen = sm.uen
            """
            
            coverage_result = db_manager.execute_query(coverage_query)
            if coverage_result:
                validation_results.update(coverage_result[0])
            
            # Top industries
            industry_query = """
                SELECT industry, COUNT(*) as company_count 
                FROM companies 
                WHERE industry IS NOT NULL 
                GROUP BY industry 
                ORDER BY company_count DESC 
                LIMIT 5
            """
            
            industry_result = db_manager.execute_query(industry_query)
            validation_results['top_industries'] = industry_result
            
            # Data quality distribution
            quality_query = """
                SELECT 
                    CASE 
                        WHEN data_quality_score >= 0.8 THEN 'High (0.8+)'
                        WHEN data_quality_score >= 0.6 THEN 'Medium (0.6-0.8)'
                        WHEN data_quality_score >= 0.4 THEN 'Low (0.4-0.6)'
                        ELSE 'Very Low (<0.4)'
                    END as quality_tier,
                    COUNT(*) as company_count
                FROM companies
                GROUP BY quality_tier
                ORDER BY MIN(data_quality_score) DESC
            """
            
            quality_result = db_manager.execute_query(quality_query)
            validation_results['quality_distribution'] = quality_result
            
        except Exception as e:
            logger.error(f"Error validating loaded data: {e}")
            validation_results['validation_error'] = str(e)
        
        return validation_results
    
    async def create_pipeline_run_record(self, pipeline_stats: Dict[str, Any]) -> str:
        """Create a record of the pipeline run"""
        try:
            run_record = {
                'id': str(uuid.uuid4()),
                'run_type': 'full',
                'status': 'completed',
                'started_at': datetime.now(),
                'completed_at': datetime.now(),
                'records_processed': pipeline_stats.get('companies_extracted', 0),
                'records_inserted': self.loaded_companies,
                'records_updated': self.updated_companies,
                'records_failed': self.failed_companies,
                'config': json.dumps({
                    'batch_size': settings.pipeline.batch_size,
                    'llm_model': settings.llm.model_name,
                    'data_sources': list(settings.data_sources.keys())
                })
            }
            
            batch_insert('pipeline_runs', [run_record])
            logger.info(f"Created pipeline run record: {run_record['id']}")
            
            return run_record['id']
            
        except Exception as e:
            logger.error(f"Error creating pipeline run record: {e}")
            return ""


# Example usage and testing
if __name__ == "__main__":
    async def test_database_loading():
        """Test database loading functionality"""
        loader = DatabaseLoader()
        
        # Sample test data
        test_companies = [
            {
                'uen': '200012345A',
                'company_name': 'Test Company Pte Ltd',
                'company_name_normalized': 'test company',
                'website': 'https://www.testcompany.com.sg',
                'industry': 'Technology',
                'company_size': 'Small (11-50)',
                'contact_email': 'info@testcompany.com.sg',
                'contact_phone': '+65 6123 4567',
                'linkedin': 'https://www.linkedin.com/company/testcompany',
                'keywords': ['technology', 'singapore', 'software'],
                'data_quality_score': 0.85,
                'source_of_data': 'Test Data'
            }
        ]
        
        print("Testing database loading...")
        
        # Test loading
        loaded_count = await loader.load_companies(test_companies)
        print(f"Loaded {loaded_count} companies")
        
        # Test validation
        validation_results = await loader.validate_loaded_data()
        print(f"Validation results: {validation_results}")
        
        # Show stats
        stats = loader.get_load_stats()
        print(f"Load stats: {stats}")
    
    asyncio.run(test_database_loading())
