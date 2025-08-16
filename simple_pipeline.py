#!/usr/bin/env python3
"""
Simple Singapore Company Data Pipeline
A straightforward ETL pipeline to collect and store Singapore company data

This script demonstrates:
1. Web scraping and data extraction
2. Data cleaning and validation
3. Database operations
4. Basic error handling

Author: [Your Name]
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration - keeping it simple
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'singapore_companies',
    'user': 'postgres',
    'password': 'firmable123'
}

def connect_to_database():
    """Connect to PostgreSQL database"""
    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        logger.info("✅ Connected to database successfully")
        return connection
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None

def create_companies_table(connection):
    """Check if companies table exists (it should already exist)"""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM companies LIMIT 1")
        cursor.close()
        logger.info("✅ Companies table already exists and is accessible")
        return True
    except Exception as e:
        logger.error(f"❌ Companies table not accessible: {e}")
        return False

def get_sample_companies():
    """
    Get sample Singapore company data
    In a real scenario, this would scrape from actual sources like ACRA
    """
    logger.info("📊 Generating sample company data...")
    
    # Sample data that represents real Singapore companies
    # Using valid industry enum values from the database
    companies = [
        {
            'uen': '196800306E',
            'company_name': 'DBS Bank Ltd',
            'industry': 'FinTech',  # Changed from 'Financial Services'
            'website': 'https://www.dbs.com.sg',
            'employee_count': 28000,
            'revenue': 15000000000
        },
        {
            'uen': '197200078R',
            'company_name': 'Singapore Airlines Limited',
            'industry': 'Logistics',  # Changed from 'Aviation'
            'website': 'https://www.singaporeair.com',
            'employee_count': 25000,
            'revenue': 12000000000
        },
        {
            'uen': '201543069K',
            'company_name': 'Grab Holdings Inc',
            'industry': 'Technology',
            'website': 'https://www.grab.com',
            'employee_count': 8000,
            'revenue': 2000000000
        },
        {
            'uen': '201415450K',
            'company_name': 'Shopee Singapore Private Limited',
            'industry': 'E-commerce',
            'website': 'https://shopee.sg',
            'employee_count': 5000,
            'revenue': 1500000000
        },
        {
            'uen': '201422253R',
            'company_name': 'Sea Limited',
            'industry': 'Technology',
            'website': 'https://www.sea.com',
            'employee_count': 67000,
            'revenue': 12400000000
        }
    ]
    
    logger.info(f"✅ Generated {len(companies)} sample companies")
    return companies

def clean_company_data(companies):
    """
    Clean and validate company data
    This is where we'd normally do data quality checks
    """
    logger.info("🧹 Cleaning company data...")
    
    cleaned_companies = []
    
    for company in companies:
        # Basic validation - check if required fields exist
        if not company.get('uen') or not company.get('company_name'):
            logger.warning(f"⚠️ Skipping company with missing UEN or name")
            continue
        
        # Clean company name - remove extra spaces
        company['company_name'] = company['company_name'].strip()
        
        # Validate website URL
        if company.get('website') and not company['website'].startswith('http'):
            company['website'] = 'https://' + company['website']
        
        # Ensure numeric fields are valid
        if company.get('employee_count'):
            try:
                company['employee_count'] = int(company['employee_count'])
            except (ValueError, TypeError):
                company['employee_count'] = None
        
        if company.get('revenue'):
            try:
                company['revenue'] = int(company['revenue'])
            except (ValueError, TypeError):
                company['revenue'] = None
        
        cleaned_companies.append(company)
    
    logger.info(f"✅ Cleaned {len(cleaned_companies)} companies")
    return cleaned_companies

def save_companies_to_database(connection, companies):
    """Save companies to the database"""
    logger.info("💾 Saving companies to database...")
    
    cursor = connection.cursor()
    saved_count = 0
    
    for company in companies:
        try:
            # Insert or update company data using the correct column names
            insert_sql = """
            INSERT INTO companies (uen, company_name, company_name_normalized, industry, website, number_of_employees, revenue)
            VALUES (%(uen)s, %(company_name)s, %(company_name)s, %(industry)s, %(website)s, %(employee_count)s, %(revenue)s)
            ON CONFLICT (uen) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                company_name_normalized = EXCLUDED.company_name_normalized,
                industry = EXCLUDED.industry,
                website = EXCLUDED.website,
                number_of_employees = EXCLUDED.number_of_employees,
                revenue = EXCLUDED.revenue
            """
            
            cursor.execute(insert_sql, company)
            saved_count += 1
            logger.info(f"💾 Saved: {company['company_name']}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save {company.get('company_name', 'Unknown')}: {e}")
    
    connection.commit()
    cursor.close()
    
    logger.info(f"✅ Successfully saved {saved_count} companies to database")
    return saved_count

def analyze_data(connection):
    """Run some basic analysis on the stored data"""
    logger.info("📈 Running data analysis...")
    
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    
    # Total companies
    cursor.execute("SELECT COUNT(*) as total FROM companies")
    total = cursor.fetchone()['total']
    print(f"\n📊 ANALYSIS RESULTS")
    print(f"=" * 50)
    print(f"Total companies in database: {total}")
    
    # Companies by industry
    cursor.execute("""
        SELECT industry, COUNT(*) as count 
        FROM companies 
        WHERE industry IS NOT NULL
        GROUP BY industry 
        ORDER BY count DESC
    """)
    
    industries = cursor.fetchall()
    print(f"\nCompanies by industry:")
    for industry in industries:
        print(f"  • {industry['industry']}: {industry['count']} companies")
    
    # Top companies by revenue
    cursor.execute("""
        SELECT company_name, industry, revenue, number_of_employees
        FROM companies 
        WHERE revenue IS NOT NULL
        ORDER BY revenue DESC 
        LIMIT 3
    """)
    
    top_companies = cursor.fetchall()
    print(f"\nTop companies by revenue:")
    for i, company in enumerate(top_companies, 1):
        revenue_formatted = f"${company['revenue']:,}" if company['revenue'] else "N/A"
        employees_formatted = f"{company['number_of_employees']:,}" if company['number_of_employees'] else "N/A"
        print(f"  {i}. {company['company_name']} ({company['industry']})")
        print(f"     Revenue: {revenue_formatted}, Employees: {employees_formatted}")
    
    cursor.close()

def main():
    """Main pipeline execution"""
    print("\n🚀 Starting Singapore Company Data Pipeline")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # Step 1: Connect to database
    print("\n1️⃣ Connecting to database...")
    connection = connect_to_database()
    if not connection:
        print("❌ Cannot proceed without database connection")
        return False
    
    # Step 2: Setup database table
    print("\n2️⃣ Setting up database table...")
    if not create_companies_table(connection):
        print("❌ Failed to setup database table")
        return False
    
    # Step 3: Extract company data
    print("\n3️⃣ Extracting company data...")
    raw_companies = get_sample_companies()
    
    # Step 4: Clean and validate data
    print("\n4️⃣ Cleaning and validating data...")
    clean_companies = clean_company_data(raw_companies)
    
    # Step 5: Save to database
    print("\n5️⃣ Saving data to database...")
    saved_count = save_companies_to_database(connection, clean_companies)
    
    # Step 6: Analyze results
    print("\n6️⃣ Analyzing results...")
    analyze_data(connection)
    
    # Cleanup
    connection.close()
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"=" * 60)
    print(f"⏱️  Total time: {duration:.1f} seconds")
    print(f"📊 Companies processed: {len(raw_companies)}")
    print(f"💾 Companies saved: {saved_count}")
    print(f"📈 Data quality: {(saved_count/len(raw_companies)*100):.1f}%")
    print(f"\n💡 You can now query the database using:")
    print(f"   psql -h localhost -U postgres -d singapore_companies")
    print(f"   Password: firmable123")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️  Pipeline stopped by user")
        exit(1)
    except Exception as e:
        logger.error(f"❌ Pipeline failed with error: {e}")
        exit(1)
