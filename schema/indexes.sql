-- Performance Indexes for Singapore Company Database
-- Optimized for common query patterns and data access

-- Primary indexes for companies table
CREATE INDEX CONCURRENTLY idx_companies_name ON companies USING gin(to_tsvector('english', company_name));
CREATE INDEX CONCURRENTLY idx_companies_name_normalized ON companies (company_name_normalized);
CREATE INDEX CONCURRENTLY idx_companies_website ON companies (website);
CREATE INDEX CONCURRENTLY idx_companies_industry ON companies (industry);
CREATE INDEX CONCURRENTLY idx_companies_size ON companies (company_size);
CREATE INDEX CONCURRENTLY idx_companies_employees ON companies (number_of_employees);
CREATE INDEX CONCURRENTLY idx_companies_revenue ON companies (revenue);
CREATE INDEX CONCURRENTLY idx_companies_founding_year ON companies (founding_year);
CREATE INDEX CONCURRENTLY idx_companies_quality_score ON companies (data_quality_score);
CREATE INDEX CONCURRENTLY idx_companies_updated ON companies (last_updated);

-- Composite indexes for common query patterns
CREATE INDEX CONCURRENTLY idx_companies_industry_size ON companies (industry, company_size);
CREATE INDEX CONCURRENTLY idx_companies_industry_employees ON companies (industry, number_of_employees);
CREATE INDEX CONCURRENTLY idx_companies_size_revenue ON companies (company_size, revenue);
CREATE INDEX CONCURRENTLY idx_companies_year_industry ON companies (founding_year, industry);

-- Full-text search indexes
CREATE INDEX CONCURRENTLY idx_companies_keywords_gin ON companies USING gin(keywords);
CREATE INDEX CONCURRENTLY idx_companies_products_gin ON companies USING gin(products_offered);
CREATE INDEX CONCURRENTLY idx_companies_services_gin ON companies USING gin(services_offered);

-- Fuzzy matching indexes using pg_trgm
CREATE INDEX CONCURRENTLY idx_companies_name_trgm ON companies USING gin(company_name gin_trgm_ops);
CREATE INDEX CONCURRENTLY idx_companies_name_norm_trgm ON companies USING gin(company_name_normalized gin_trgm_ops);

-- Social media table indexes
CREATE INDEX CONCURRENTLY idx_social_media_uen ON company_social_media (uen);
CREATE INDEX CONCURRENTLY idx_social_media_linkedin ON company_social_media (linkedin) WHERE linkedin IS NOT NULL;
CREATE INDEX CONCURRENTLY idx_social_media_facebook ON company_social_media (facebook) WHERE facebook IS NOT NULL;
CREATE INDEX CONCURRENTLY idx_social_media_updated ON company_social_media (updated_at);

-- Financial table indexes
CREATE INDEX CONCURRENTLY idx_financials_uen ON company_financials (uen);
CREATE INDEX CONCURRENTLY idx_financials_revenue ON company_financials (annual_revenue);
CREATE INDEX CONCURRENTLY idx_financials_revenue_year ON company_financials (revenue_year);
CREATE INDEX CONCURRENTLY idx_financials_funding ON company_financials (funding_raised);
CREATE INDEX CONCURRENTLY idx_financials_valuation ON company_financials (valuation);
CREATE INDEX CONCURRENTLY idx_financials_profitable ON company_financials (is_profitable);

-- Location table indexes
CREATE INDEX CONCURRENTLY idx_locations_uen ON company_locations (uen);
CREATE INDEX CONCURRENTLY idx_locations_type ON company_locations (location_type);
CREATE INDEX CONCURRENTLY idx_locations_postal ON company_locations (postal_code);
CREATE INDEX CONCURRENTLY idx_locations_city ON company_locations (city);
CREATE INDEX CONCURRENTLY idx_locations_primary ON company_locations (is_primary) WHERE is_primary = true;
CREATE INDEX CONCURRENTLY idx_locations_coordinates ON company_locations (latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Data source tracking indexes
CREATE INDEX CONCURRENTLY idx_data_sources_name ON data_sources (source_name);
CREATE INDEX CONCURRENTLY idx_data_sources_type ON data_sources (source_type);
CREATE INDEX CONCURRENTLY idx_data_sources_active ON data_sources (is_active) WHERE is_active = true;
CREATE INDEX CONCURRENTLY idx_data_sources_reliability ON data_sources (reliability_score);

-- Field source tracking indexes
CREATE INDEX CONCURRENTLY idx_field_sources_uen ON company_field_sources (uen);
CREATE INDEX CONCURRENTLY idx_field_sources_field ON company_field_sources (field_name);
CREATE INDEX CONCURRENTLY idx_field_sources_source ON company_field_sources (source_id);
CREATE INDEX CONCURRENTLY idx_field_sources_confidence ON company_field_sources (confidence_score);
CREATE INDEX CONCURRENTLY idx_field_sources_extracted ON company_field_sources (extracted_at);

-- Entity matching indexes
CREATE INDEX CONCURRENTLY idx_entity_matches_primary ON entity_matches (primary_uen);
CREATE INDEX CONCURRENTLY idx_entity_matches_duplicate ON entity_matches (duplicate_uen);
CREATE INDEX CONCURRENTLY idx_entity_matches_type ON entity_matches (match_type);
CREATE INDEX CONCURRENTLY idx_entity_matches_score ON entity_matches (match_score);
CREATE INDEX CONCURRENTLY idx_entity_matches_resolved ON entity_matches (resolved_at);

-- Data quality log indexes
CREATE INDEX CONCURRENTLY idx_quality_log_uen ON data_quality_log (uen);
CREATE INDEX CONCURRENTLY idx_quality_log_type ON data_quality_log (check_type);
CREATE INDEX CONCURRENTLY idx_quality_log_result ON data_quality_log (check_result);
CREATE INDEX CONCURRENTLY idx_quality_log_checked ON data_quality_log (checked_at);

-- Pipeline execution indexes
CREATE INDEX CONCURRENTLY idx_pipeline_runs_type ON pipeline_runs (run_type);
CREATE INDEX CONCURRENTLY idx_pipeline_runs_status ON pipeline_runs (status);
CREATE INDEX CONCURRENTLY idx_pipeline_runs_started ON pipeline_runs (started_at);
CREATE INDEX CONCURRENTLY idx_pipeline_runs_completed ON pipeline_runs (completed_at);

-- LLM processing log indexes
CREATE INDEX CONCURRENTLY idx_llm_log_uen ON llm_processing_log (uen);
CREATE INDEX CONCURRENTLY idx_llm_log_model ON llm_processing_log (model_name);
CREATE INDEX CONCURRENTLY idx_llm_log_prompt_type ON llm_processing_log (prompt_type);
CREATE INDEX CONCURRENTLY idx_llm_log_confidence ON llm_processing_log (confidence_score);
CREATE INDEX CONCURRENTLY idx_llm_log_processed ON llm_processing_log (processed_at);
CREATE INDEX CONCURRENTLY idx_llm_log_tokens ON llm_processing_log (tokens_used);

-- Partial indexes for common filters
CREATE INDEX CONCURRENTLY idx_companies_with_website ON companies (uen) WHERE website IS NOT NULL;
CREATE INDEX CONCURRENTLY idx_companies_with_revenue ON companies (uen) WHERE revenue IS NOT NULL;
CREATE INDEX CONCURRENTLY idx_companies_with_employees ON companies (uen) WHERE number_of_employees IS NOT NULL;
CREATE INDEX CONCURRENTLY idx_companies_high_quality ON companies (uen) WHERE data_quality_score >= 0.8;
CREATE INDEX CONCURRENTLY idx_companies_recent ON companies (uen) WHERE founding_year >= 2000;

-- Expression indexes for common calculations
CREATE INDEX CONCURRENTLY idx_companies_name_length ON companies (length(company_name));
CREATE INDEX CONCURRENTLY idx_companies_website_domain ON companies (substring(website from 'https?://(?:www\.)?([^/]+)'));

-- Indexes for analytics queries
CREATE INDEX CONCURRENTLY idx_analytics_industry_year ON companies (industry, founding_year) WHERE founding_year IS NOT NULL;
CREATE INDEX CONCURRENTLY idx_analytics_size_employees ON companies (company_size, number_of_employees) WHERE number_of_employees IS NOT NULL;
CREATE INDEX CONCURRENTLY idx_analytics_revenue_industry ON companies (revenue, industry) WHERE revenue IS NOT NULL;

-- Statistics collection for query optimization
ANALYZE companies;
ANALYZE company_social_media;
ANALYZE company_financials;
ANALYZE company_locations;
ANALYZE data_sources;
ANALYZE company_field_sources;
ANALYZE entity_matches;
ANALYZE data_quality_log;
ANALYZE pipeline_runs;
ANALYZE llm_processing_log;

-- Refresh materialized view
REFRESH MATERIALIZED VIEW company_analytics;

-- Create index on materialized view
CREATE INDEX CONCURRENTLY idx_company_analytics_industry ON company_analytics (industry);
CREATE INDEX CONCURRENTLY idx_company_analytics_size ON company_analytics (company_size);
CREATE INDEX CONCURRENTLY idx_company_analytics_year ON company_analytics (founding_year);
CREATE INDEX CONCURRENTLY idx_company_analytics_count ON company_analytics (company_count);

-- Performance monitoring queries (for reference)
/*
-- Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
ORDER BY idx_scan DESC;

-- Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check slow queries
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    rows
FROM pg_stat_statements 
ORDER BY mean_time DESC 
LIMIT 10;
*/
