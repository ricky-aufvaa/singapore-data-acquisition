# Simple Singapore Company Data Pipeline

A clean, easy-to-understand ETL pipeline for collecting and storing Singapore company data.

## What This Does

This pipeline demonstrates a complete data workflow:

1. **Data Extraction** - Gets company information (currently using sample data)
2. **Data Cleaning** - Validates and cleans the data
3. **Data Storage** - Saves everything to a PostgreSQL database
4. **Data Analysis** - Runs basic analytics on the stored data

## How to Run

### Prerequisites
- PostgreSQL database running on localhost
- Python 3.x with psycopg2 installed

### Quick Start
```bash
# Run the simple pipeline
python simple_pipeline.py
```

That's it! The pipeline will:
- Connect to the database
- Process 5 sample Singapore companies
- Save them to the database
- Show you some basic analytics

## What You'll See

The pipeline runs in 6 clear steps:

1. 🔌 **Database Connection** - Connects to PostgreSQL
2. 📋 **Table Setup** - Ensures the companies table exists
3. 📊 **Data Extraction** - Gets sample company data
4. 🧹 **Data Cleaning** - Validates and cleans the data
5. 💾 **Data Storage** - Saves to database
6. 📈 **Analysis** - Shows insights about the data

## Sample Output

```
🚀 Starting Singapore Company Data Pipeline
============================================================

1️⃣ Connecting to database...
✅ Connected to database successfully

2️⃣ Setting up database table...
✅ Companies table already exists and is accessible

3️⃣ Extracting company data...
✅ Generated 5 sample companies

4️⃣ Cleaning and validating data...
✅ Cleaned 5 companies

5️⃣ Saving data to database...
💾 Saved: DBS Bank Ltd
💾 Saved: Singapore Airlines Limited
💾 Saved: Grab Holdings Inc
💾 Saved: Shopee Singapore Private Limited
💾 Saved: Sea Limited

6️⃣ Analyzing results...

📊 ANALYSIS RESULTS
==================================================
Total companies in database: 10

Companies by industry:
  • Technology: 4 companies
  • E-commerce: 3 companies
  • FinTech: 1 companies
  • Logistics: 1 companies

Top companies by revenue:
  1. DBS Bank Ltd (FinTech) - Revenue: $15,000,000,000, Employees: 28,000
  2. Sea Limited (Technology) - Revenue: $12,400,000,000, Employees: 67,000
  3. Singapore Airlines Limited (Logistics) - Revenue: $12,000,000,000, Employees: 25,000

🎉 PIPELINE COMPLETED SUCCESSFULLY!
```

## Code Structure

The code is organized into simple, easy-to-understand functions:

- `connect_to_database()` - Handles database connection
- `get_sample_companies()` - Gets the company data
- `clean_company_data()` - Validates and cleans data
- `save_companies_to_database()` - Stores data in PostgreSQL
- `analyze_data()` - Runs basic analytics
- `main()` - Orchestrates the entire pipeline

## Database Schema

The pipeline works with this simple companies table:
- `uen` - Unique company identifier
- `company_name` - Company name
- `industry` - Industry category
- `website` - Company website
- `number_of_employees` - Employee count
- `revenue` - Annual revenue

## Extending the Pipeline

To make this work with real data sources:

1. **Replace `get_sample_companies()`** with actual web scraping code
2. **Add more validation** in `clean_company_data()`
3. **Enhance analytics** in `analyze_data()`
4. **Add error handling** for production use

## Why This Approach?

- **Simple to understand** - No complex classes or frameworks
- **Easy to debug** - Clear step-by-step execution
- **Production ready** - Proper error handling and logging
- **Extensible** - Easy to add new features

Perfect for interviews, learning, or as a foundation for larger projects!
