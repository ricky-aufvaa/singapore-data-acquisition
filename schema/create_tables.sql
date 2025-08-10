-- Singapore Company Intelligence Database Schema
-- Created for Data Engineer Assessment
-- Optimized for 50,000+ companies with comprehensive data tracking

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "unaccent";

-- Create custom types
CREATE TYPE company_size_enum AS ENUM (
    'Micro (1-10)', 
    'Small (11-50)', 
    'Medium (51-200)', 
    'Large (201-1000)', 
    'Enterprise (1000+)',
    'Unknown'
);

CREATE TYPE industry_enum AS ENUM (
    'Technology',
    'FinTech',
    'Healthcare',
    'E-commerce',
    'Manufacturing',
    'Professional Services',
    'Real Estate',
    'F&B',
    'Education',
    'Logistics',
    'Construction',
    'Retail',
    'Energy',
    'Media',
    'Automotive',
    'Agriculture',
    'Tourism',
    'Government',
    'Non-Profit',
    'Other'
);

-- Main companies table
CREATE TABLE companies (
    uen VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    company_name_normalized VARCHAR(255),
    website VARCHAR(500),
    hq_country VARCHAR(50) DEFAULT 'Singapore',
    no_of_locations_in_singapore INTEGER DEFAULT 1,
    industry industry_enum,
    number_of_employees INTEGER,
    company_size company_size_enum DEFAULT 'Unknown',
    is_it_delisted BOOLEAN DEFAULT FALSE,
    stock_exchange_code VARCHAR(20),
    revenue DECIMAL(15,2),
    founding_year INTEGER,
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    
    -- AI-generated fields
    keywords TEXT[],
    products_offered TEXT[],
    services_offered TEXT[],
    
    -- Data quality and tracking
    data_quality_score DECIMAL(3,2) DEFAULT 0.0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_founding_year CHECK (founding_year >= 1800 AND founding_year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    CONSTRAINT valid_employees CHECK (number_of_employees >= 0),
    CONSTRAINT valid_revenue CHECK (revenue >= 0),
    CONSTRAINT valid_quality_score CHECK (data_quality_score >= 0.0 AND data_quality_score <= 1.0)
);

-- Social media and digital presence
CREATE TABLE company_social_media (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    uen VARCHAR(20) REFERENCES companies(uen) ON DELETE CASCADE,
    linkedin VARCHAR(500),
    facebook VARCHAR(500),
    instagram VARCHAR(500),
    twitter VARCHAR(500),
    youtube VARCHAR(500),
    tiktok VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Financial information
CREATE TABLE company_financials (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    uen VARCHAR(20) REFERENCES companies(uen) ON DELETE CASCADE,
    annual_revenue DECIMAL(15,2),
    revenue_currency VARCHAR(3) DEFAULT 'SGD',
    revenue_year INTEGER,
    funding_raised DECIMAL(15,2),
    funding_currency VARCHAR(3) DEFAULT 'SGD',
    last_funding_round VARCHAR(50),
    last_funding_date DATE,
    valuation DECIMAL(15,2),
    is_profitable BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Company locations and addresses
CREATE TABLE company_locations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    uen VARCHAR(20) REFERENCES companies(uen) ON DELETE CASCADE,
    location_type VARCHAR(50) DEFAULT 'headquarters', -- headquarters, branch, office
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    postal_code VARCHAR(10),
    city VARCHAR(100) DEFAULT 'Singapore',
    state VARCHAR(100),
    country VARCHAR(50) DEFAULT 'Singapore',
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data source tracking for lineage
CREATE TABLE data_sources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL, -- api, scraping, manual, llm
    source_url VARCHAR(500),
    access_method VARCHAR(100),
    reliability_score DECIMAL(3,2) DEFAULT 0.8,
    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Field-level source tracking
CREATE TABLE company_field_sources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    uen VARCHAR(20) REFERENCES companies(uen) ON DELETE CASCADE,
    field_name VARCHAR(100) NOT NULL,
    source_id UUID REFERENCES data_sources(id),
    confidence_score DECIMAL(3,2) DEFAULT 0.8,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(uen, field_name, source_id)
);

-- Entity matching and deduplication tracking
CREATE TABLE entity_matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    primary_uen VARCHAR(20) REFERENCES companies(uen) ON DELETE CASCADE,
    duplicate_uen VARCHAR(20),
    match_type VARCHAR(50), -- exact, fuzzy, manual
    match_score DECIMAL(3,2),
    match_algorithm VARCHAR(100),
    resolved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_by VARCHAR(100) DEFAULT 'system'
);

-- Data quality audit log
CREATE TABLE data_quality_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    uen VARCHAR(20) REFERENCES companies(uen) ON DELETE CASCADE,
    check_type VARCHAR(100) NOT NULL,
    check_result VARCHAR(20) NOT NULL, -- pass, fail, warning
    check_details JSONB,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline execution tracking
CREATE TABLE pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_type VARCHAR(50) NOT NULL, -- full, incremental, backfill
    status VARCHAR(20) NOT NULL, -- running, completed, failed
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_details JSONB,
    config JSONB
);

-- LLM processing log
CREATE TABLE llm_processing_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    uen VARCHAR(20) REFERENCES companies(uen) ON DELETE CASCADE,
    model_name VARCHAR(100) NOT NULL,
    prompt_type VARCHAR(50) NOT NULL, -- industry_classification, keyword_extraction, etc.
    input_text TEXT,
    output_text TEXT,
    confidence_score DECIMAL(3,2),
    processing_time_ms INTEGER,
    tokens_used INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create materialized view for analytics
CREATE MATERIALIZED VIEW company_analytics AS
SELECT 
    c.industry,
    c.company_size,
    c.founding_year,
    COUNT(*) as company_count,
    AVG(c.number_of_employees) as avg_employees,
    AVG(c.revenue) as avg_revenue,
    COUNT(CASE WHEN c.website IS NOT NULL THEN 1 END) as companies_with_website,
    COUNT(CASE WHEN sm.linkedin IS NOT NULL THEN 1 END) as companies_with_linkedin,
    AVG(c.data_quality_score) as avg_quality_score
FROM companies c
LEFT JOIN company_social_media sm ON c.uen = sm.uen
GROUP BY c.industry, c.company_size, c.founding_year;

-- Create function to update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for automatic timestamp updates
CREATE TRIGGER update_company_social_media_updated_at 
    BEFORE UPDATE ON company_social_media 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_company_financials_updated_at 
    BEFORE UPDATE ON company_financials 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create function to calculate data quality score
CREATE OR REPLACE FUNCTION calculate_data_quality_score(company_uen VARCHAR(20))
RETURNS DECIMAL(3,2) AS $$
DECLARE
    score DECIMAL(3,2) := 0.0;
    field_count INTEGER := 0;
    filled_fields INTEGER := 0;
BEGIN
    -- Count total fields and filled fields for the company
    SELECT 
        20 as total_fields, -- Total number of key fields we track
        (CASE WHEN company_name IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN number_of_employees IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN company_size IS NOT NULL AND company_size != 'Unknown' THEN 1 ELSE 0 END +
         CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN founding_year IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN contact_email IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN contact_phone IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN keywords IS NOT NULL AND array_length(keywords, 1) > 0 THEN 1 ELSE 0 END +
         CASE WHEN products_offered IS NOT NULL AND array_length(products_offered, 1) > 0 THEN 1 ELSE 0 END +
         CASE WHEN services_offered IS NOT NULL AND array_length(services_offered, 1) > 0 THEN 1 ELSE 0 END) as filled
    INTO field_count, filled_fields
    FROM companies 
    WHERE uen = company_uen;
    
    -- Calculate score as percentage of filled fields
    IF field_count > 0 THEN
        score := ROUND((filled_fields::DECIMAL / field_count::DECIMAL), 2);
    END IF;
    
    RETURN score;
END;
$$ LANGUAGE plpgsql;

-- Create function to update data quality scores
CREATE OR REPLACE FUNCTION update_data_quality_scores()
RETURNS INTEGER AS $$
DECLARE
    company_record RECORD;
    updated_count INTEGER := 0;
BEGIN
    FOR company_record IN SELECT uen FROM companies LOOP
        UPDATE companies 
        SET data_quality_score = calculate_data_quality_score(company_record.uen)
        WHERE uen = company_record.uen;
        updated_count := updated_count + 1;
    END LOOP;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Insert initial data sources
INSERT INTO data_sources (source_name, source_type, source_url, access_method, reliability_score) VALUES
('ACRA Business Registry', 'api', 'https://www.acra.gov.sg', 'Web Portal/API', 0.95),
('Yellow Pages Singapore', 'scraping', 'https://www.yellowpages.com.sg', 'Web Scraping', 0.80),
('LinkedIn Company Pages', 'api', 'https://www.linkedin.com', 'API/Scraping', 0.85),
('SGX Listed Companies', 'api', 'https://www.sgx.com', 'API', 0.90),
('Crunchbase', 'api', 'https://www.crunchbase.com', 'API', 0.85),
('Company Websites', 'scraping', 'Various', 'Web Scraping', 0.75),
('Government Tenders', 'scraping', 'https://www.gebiz.gov.sg', 'Web Scraping', 0.80),
('Llama 3 LLM', 'llm', 'Local Ollama', 'AI Processing', 0.70);

-- Create comments for documentation
COMMENT ON TABLE companies IS 'Main table storing core company information with UEN as primary key';
COMMENT ON TABLE company_social_media IS 'Social media profiles and digital presence for companies';
COMMENT ON TABLE company_financials IS 'Financial information including revenue, funding, and valuation';
COMMENT ON TABLE company_locations IS 'Physical locations and addresses for company offices';
COMMENT ON TABLE data_sources IS 'Registry of all data sources used in the pipeline';
COMMENT ON TABLE company_field_sources IS 'Field-level tracking of data source lineage';
COMMENT ON TABLE entity_matches IS 'Record of entity matching and deduplication decisions';
COMMENT ON TABLE data_quality_log IS 'Audit log for data quality checks and validations';
COMMENT ON TABLE pipeline_runs IS 'Execution history and metrics for ETL pipeline runs';
COMMENT ON TABLE llm_processing_log IS 'Log of all LLM processing operations and results';

COMMENT ON COLUMN companies.uen IS 'Unique Entity Number - Singapore business registration identifier';
COMMENT ON COLUMN companies.company_name_normalized IS 'Normalized company name for fuzzy matching';
COMMENT ON COLUMN companies.data_quality_score IS 'Calculated score (0-1) based on data completeness';
COMMENT ON COLUMN companies.keywords IS 'AI-generated keywords from company description and website';
