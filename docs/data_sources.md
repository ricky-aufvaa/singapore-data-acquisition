# Data Sources Documentation - Singapore Company Database

## Overview

This document provides comprehensive documentation of all data sources used in the Singapore Company Database ETL pipeline. Each source is analyzed for coverage, reliability, access methods, and legal compliance status.

## Data Source Summary

| Source Category | Number of Sources | Total Coverage | Primary Use Case |
|----------------|------------------|----------------|------------------|
| **Official Government** | 3 | 500K+ entities | UEN, official data |
| **Business Directories** | 4 | 180K+ companies | Contact info, websites |
| **Social Media** | 2 | 75K+ profiles | Employee count, social presence |
| **Financial/Investment** | 3 | 8K+ companies | Revenue, funding data |
| **Industry-Specific** | 2 | 2.5K+ companies | Specialized data |
| **Company Websites** | 1 | 200K+ potential | Primary source data |

## Detailed Source Analysis

### 1. Official Government Sources

#### ACRA Business Registry
- **URL**: https://www.acra.gov.sg
- **Type**: Official Government Registry
- **Access Method**: Hybrid (API + Web Portal)
- **Coverage**: 500,000+ registered entities
- **Reliability Score**: 95%
- **Legal Status**: ✅ Compliant
- **Cost**: Free (basic searches), Paid (bulk data)

**Data Fields Available:**
- ✅ UEN (Unique Entity Number)
- ✅ Company Name
- ✅ Registration Date
- ✅ Company Status
- ✅ Industry Classification
- ✅ Registered Address
- ✅ Share Capital
- ✅ Directors Information

**Technical Implementation:**
```python
# ACRA API integration example
async def extract_from_acra(self, uen: str):
    url = f"https://www.acra.gov.sg/api/company/{uen}"
    headers = {"Authorization": f"Bearer {api_key}"}
    response = await self.session.get(url, headers=headers)
    return response.json()
```

**Rate Limits**: 100 requests/minute for API access
**Robots.txt Status**: ✅ Compliant
**Notes**: Primary source for UEN validation and official company data

---

#### SGX Listed Companies
- **URL**: https://www.sgx.com
- **Type**: Financial Exchange
- **Access Method**: API + Data Feeds
- **Coverage**: 700+ public companies
- **Reliability Score**: 90%
- **Legal Status**: ✅ Compliant
- **Cost**: Free (basic data), Paid (real-time feeds)

**Data Fields Available:**
- ✅ Stock Exchange Code
- ✅ Market Capitalization
- ✅ Financial Data
- ✅ Annual Reports
- ✅ Trading Status
- ✅ Sector Classification

**Technical Implementation:**
```python
# SGX API integration
async def extract_from_sgx(self):
    url = "https://api.sgx.com/securities/v1/companies"
    response = await self.session.get(url)
    return response.json()
```

**Rate Limits**: 1000 requests/hour
**Update Frequency**: Real-time during trading hours
**Notes**: Essential for public company financial data

---

#### GeBIZ Government Tenders
- **URL**: https://www.gebiz.gov.sg
- **Type**: Government Procurement Portal
- **Access Method**: Web Scraping
- **Coverage**: 10,000+ vendors
- **Reliability Score**: 80%
- **Legal Status**: ✅ Compliant
- **Cost**: Free

**Data Fields Available:**
- ✅ Vendor Information
- ✅ Contract Awards
- ✅ Business Capabilities
- ✅ Tender History
- ✅ Company Size Indicators

**Technical Implementation:**
```python
# GeBIZ scraping approach
async def scrape_gebiz_vendors(self):
    base_url = "https://www.gebiz.gov.sg/ptn/opportunity/BOListing.xhtml"
    # Implement respectful scraping with delays
    await asyncio.sleep(2)  # Rate limiting
```

**Scraping Considerations**: Requires JavaScript rendering, pagination handling
**Update Frequency**: Daily
**Notes**: Valuable for B2G companies and capability assessment

---

### 2. Business Directories

#### Yellow Pages Singapore
- **URL**: https://www.yellowpages.com.sg
- **Type**: Business Directory
- **Access Method**: Web Scraping
- **Coverage**: 100,000+ companies
- **Reliability Score**: 75%
- **Legal Status**: ⚠️ Requires Review
- **Cost**: Free

**Data Fields Available:**
- ✅ Company Name
- ✅ Contact Information
- ✅ Address
- ✅ Website URL
- ✅ Business Category
- ✅ Operating Hours
- ✅ Customer Reviews

**Technical Implementation:**
```python
# Yellow Pages scraping strategy
async def scrape_yellowpages_category(self, category: str):
    url = f"https://www.yellowpages.com.sg/category/{category}"
    # Implement pagination and rate limiting
    for page in range(1, max_pages):
        await self.rate_limiter.wait()
        # Extract company listings
```

**Rate Limits**: 2 requests/second recommended
**Robots.txt Status**: ⚠️ Restrictive - requires careful compliance
**Legal Considerations**: Terms of Service review required
**Notes**: Rich source for SME contact information

---

#### Singapore Business Directory
- **URL**: https://www.sgbusinessdirectory.com
- **Type**: Commercial Directory
- **Access Method**: Web Scraping
- **Coverage**: 50,000+ companies
- **Reliability Score**: 70%
- **Legal Status**: ⚠️ Requires Review
- **Cost**: Free

**Data Fields Available:**
- ✅ Company Profiles
- ✅ Contact Details
- ✅ Services Offered
- ✅ Business Description
- ✅ Location Information

**Scraping Strategy**: Category-based extraction with pagination
**Update Frequency**: Weekly
**Notes**: Good supplementary source for company descriptions

---

#### Kompass Singapore
- **URL**: https://sg.kompass.com
- **Type**: B2B Directory
- **Access Method**: Web Scraping
- **Coverage**: 30,000+ companies
- **Reliability Score**: 75%
- **Legal Status**: ⚠️ Requires Review
- **Cost**: Free (basic), Paid (premium data)

**Data Fields Available:**
- ✅ Company Details
- ✅ Products & Services
- ✅ Trade Data
- ✅ Import/Export Information
- ✅ Employee Count Estimates

**Technical Challenges**: Anti-bot measures, CAPTCHA protection
**Notes**: Valuable for manufacturing and trading companies

---

### 3. Social Media & Professional Networks

#### LinkedIn Company Pages
- **URL**: https://www.linkedin.com
- **Type**: Professional Network
- **Access Method**: API (Official)
- **Coverage**: 50,000+ Singapore company profiles
- **Reliability Score**: 85%
- **Legal Status**: ⚠️ Requires Review (API Terms)
- **Cost**: Paid API access required

**Data Fields Available:**
- ✅ Employee Count
- ✅ Industry Classification
- ✅ Company Size
- ✅ Company Updates
- ✅ Follower Count
- ✅ Headquarters Location
- ✅ Founded Year

**API Implementation:**
```python
# LinkedIn API integration
async def get_linkedin_company(self, company_id: str):
    url = f"https://api.linkedin.com/v2/companies/{company_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = await self.session.get(url, headers=headers)
    return response.json()
```

**Rate Limits**: Very strict - 500 requests/day for basic tier
**API Requirements**: OAuth 2.0, approved application
**Notes**: Most reliable source for employee count and company size

---

#### Facebook Business Pages
- **URL**: https://www.facebook.com
- **Type**: Social Media Platform
- **Access Method**: Graph API
- **Coverage**: 25,000+ Singapore business pages
- **Reliability Score**: 70%
- **Legal Status**: ⚠️ Requires Review
- **Cost**: Free (basic), Paid (advanced features)

**Data Fields Available:**
- ✅ Page Information
- ✅ Contact Details
- ✅ Business Hours
- ✅ Customer Reviews
- ✅ Check-ins
- ✅ About Section

**Rate Limits**: 200 requests/hour per app
**Notes**: Good for F&B and retail businesses

---

### 4. Financial & Investment Data

#### Crunchbase
- **URL**: https://www.crunchbase.com
- **Type**: Startup & Investment Database
- **Access Method**: API
- **Coverage**: 5,000+ Singapore startups
- **Reliability Score**: 85%
- **Legal Status**: ✅ Compliant
- **Cost**: Paid API ($500+/month)

**Data Fields Available:**
- ✅ Funding Data
- ✅ Investor Information
- ✅ Startup Profiles
- ✅ Acquisition History
- ✅ Key Personnel
- ✅ Technology Stack

**API Implementation:**
```python
# Crunchbase API integration
async def get_crunchbase_company(self, company_name: str):
    url = f"https://api.crunchbase.com/v3.1/organizations/{company_name}"
    headers = {"X-cb-user-key": api_key}
    response = await self.session.get(url, headers=headers)
    return response.json()
```

**Rate Limits**: 1000 requests/day (basic tier)
**Notes**: Essential for startup ecosystem analysis

---

#### AngelList (Wellfound)
- **URL**: https://wellfound.com
- **Type**: Startup Platform
- **Access Method**: Web Scraping
- **Coverage**: 3,000+ Singapore startups
- **Reliability Score**: 75%
- **Legal Status**: ⚠️ Requires Review
- **Cost**: Free

**Data Fields Available:**
- ✅ Startup Jobs
- ✅ Company Profiles
- ✅ Funding Status
- ✅ Team Size
- ✅ Technology Stack

**Scraping Challenges**: Dynamic content, rate limiting
**Notes**: Good for early-stage startup data

---

### 5. Industry-Specific Sources

#### FinTech Singapore Directory
- **URL**: https://fintechsingapore.org
- **Type**: Industry Association
- **Access Method**: Web Scraping
- **Coverage**: 500+ FinTech companies
- **Reliability Score**: 80%
- **Legal Status**: ✅ Compliant
- **Cost**: Free

**Data Fields Available:**
- ✅ FinTech Companies
- ✅ Services Offered
- ✅ Partnership Information
- ✅ Regulatory Status

**Notes**: Authoritative source for FinTech sector

---

#### Singapore Manufacturing Federation
- **URL**: https://www.smfederation.org.sg
- **Type**: Industry Federation
- **Access Method**: Web Scraping
- **Coverage**: 2,000+ manufacturers
- **Reliability Score**: 80%
- **Legal Status**: ✅ Compliant
- **Cost**: Free

**Data Fields Available:**
- ✅ Manufacturing Companies
- ✅ Capabilities
- ✅ Certifications
- ✅ Export Markets

**Notes**: Comprehensive manufacturing sector coverage

---

### 6. Company Websites (Primary Source)

#### Individual Company Websites
- **Coverage**: 200,000+ potential websites
- **Access Method**: Web Scraping
- **Reliability Score**: 75% (varies by site)
- **Legal Status**: ⚠️ Requires Individual Review
- **Cost**: Free

**Data Fields Available:**
- ✅ About Us Information
- ✅ Products & Services
- ✅ Contact Information
- ✅ Team Information
- ✅ News & Updates
- ✅ Career Information

**Technical Implementation:**
```python
# Website scraping approach
async def scrape_company_website(self, url: str):
    # Check robots.txt compliance
    if not await self.check_robots_compliance(url):
        return None
    
    # Rate limiting
    await self.rate_limiter.wait_for_url(url)
    
    # Extract data
    response = await self.session.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    return self.extract_company_data(soup)
```

**Challenges**: 
- Diverse website structures
- Anti-bot measures
- Dynamic content (JavaScript)
- Rate limiting requirements

**Success Strategies**:
- Respectful crawling delays
- User-agent rotation
- Headless browser for dynamic content
- Structured data extraction (JSON-LD)

---

## Data Quality Assessment

### Coverage Analysis

| Data Field | Primary Sources | Coverage Estimate | Quality Score |
|------------|----------------|------------------|---------------|
| **UEN** | ACRA | 95% | 9.5/10 |
| **Company Name** | ACRA, Directories | 98% | 9.0/10 |
| **Website** | Directories, Websites | 70% | 8.0/10 |
| **Industry** | ACRA, LinkedIn | 85% | 8.5/10 |
| **Employee Count** | LinkedIn | 40% | 7.5/10 |
| **Contact Email** | Websites, Directories | 60% | 7.0/10 |
| **Contact Phone** | Directories, Websites | 65% | 7.5/10 |
| **Social Media** | LinkedIn, Facebook | 45% | 7.0/10 |
| **Financial Data** | SGX, Crunchbase | 15% | 9.0/10 |
| **Keywords** | LLM Generated | 80% | 7.5/10 |

### Reliability Scoring Methodology

**9.0-10.0**: Official government sources, verified data
**8.0-8.9**: Established platforms with verification processes
**7.0-7.9**: Commercial directories, self-reported data
**6.0-6.9**: Social media, user-generated content
**5.0-5.9**: Scraped data, unverified sources

---

## Legal & Compliance Framework

### Compliance Status Summary

**✅ Fully Compliant (5 sources)**
- ACRA Business Registry
- SGX Listed Companies  
- GeBIZ Government Tenders
- FinTech Singapore Directory
- Singapore Manufacturing Federation

**⚠️ Requires Review (7 sources)**
- Yellow Pages Singapore
- Singapore Business Directory
- Kompass Singapore
- LinkedIn Company Pages
- Facebook Business Pages
- AngelList
- Company Websites

### Legal Considerations

#### Data Protection (PDPA Compliance)
- **Personal Data Handling**: Minimize collection of personal information
- **Consent Mechanisms**: Ensure proper consent for data processing
- **Data Retention**: Implement appropriate retention policies
- **Cross-border Transfer**: Comply with international data transfer rules

#### Terms of Service Compliance
- **Robots.txt Adherence**: Check and respect robots.txt files
- **Rate Limiting**: Implement respectful crawling delays
- **Attribution**: Provide proper attribution where required
- **Commercial Use**: Review restrictions on commercial usage

#### Recommended Actions
1. **Legal Review**: Conduct thorough ToS review for each source
2. **Robots.txt Checking**: Implement automated robots.txt compliance
3. **Rate Limiting**: Enforce respectful crawling delays (1-2 seconds)
4. **User-Agent Identification**: Use descriptive user-agent strings
5. **Contact Information**: Provide contact details in user-agent
6. **Partnership Opportunities**: Explore official data partnerships

---

## Technical Implementation Guidelines

### Rate Limiting Strategy

```python
# Domain-specific rate limits
RATE_LIMITS = {
    'linkedin.com': {'requests_per_second': 0.5, 'requests_per_minute': 20},
    'facebook.com': {'requests_per_second': 1, 'requests_per_minute': 30},
    'yellowpages.com.sg': {'requests_per_second': 2, 'requests_per_minute': 100},
    'acra.gov.sg': {'requests_per_second': 1, 'requests_per_minute': 50},
    'default': {'requests_per_second': 2, 'requests_per_minute': 100}
}
```

### Error Handling Strategy

```python
# Robust error handling
async def extract_with_retry(self, source_func, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await source_func()
        except RateLimitError:
            wait_time = 2 ** attempt  # Exponential backoff
            await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
```

### Data Validation Framework

```python
# Data quality validation
def validate_company_data(company: Dict[str, Any]) -> Dict[str, Any]:
    validation_results = {
        'is_valid': True,
        'issues': [],
        'quality_score': 0.0
    }
    
    # UEN validation
    if not validate_singapore_uen(company.get('uen')):
        validation_results['issues'].append('Invalid UEN format')
    
    # Email validation
    if company.get('contact_email'):
        if not validate_email_format(company['contact_email']):
            validation_results['issues'].append('Invalid email format')
    
    # Calculate quality score
    validation_results['quality_score'] = calculate_quality_score(company)
    
    return validation_results
```

---

## Monitoring & Maintenance

### Data Freshness Monitoring

| Source | Update Frequency | Monitoring Method |
|--------|-----------------|-------------------|
| ACRA | Real-time | API status checks |
| SGX | Real-time | Market data feeds |
| LinkedIn | Daily | Profile change detection |
| Directories | Weekly | Content hash comparison |
| Websites | Monthly | Sitemap monitoring |

### Quality Metrics Tracking

```python
# Quality metrics collection
QUALITY_METRICS = {
    'extraction_success_rate': 0.95,  # Target: 95%
    'data_completeness_rate': 0.80,   # Target: 80%
    'duplicate_detection_rate': 0.99, # Target: 99%
    'validation_pass_rate': 0.90,     # Target: 90%
    'source_availability': 0.98       # Target: 98%
}
```

### Maintenance Schedule

**Daily**:
- Monitor extraction success rates
- Check API status and rate limits
- Review error logs

**Weekly**:
- Update directory sources
- Validate data quality metrics
- Review legal compliance status

**Monthly**:
- Comprehensive website re-crawling
- Source reliability assessment
- Legal compliance audit

**Quarterly**:
- Source strategy review
- New source identification
- Technology stack updates

---

## Future Enhancements

### Planned Source Additions

1. **Government Procurement Data**: Expand GeBIZ coverage
2. **Industry Association Directories**: Add more sector-specific sources
3. **News & Media Sources**: Company mention tracking
4. **Patent Databases**: Innovation indicators
5. **Trade Data**: Import/export information

### Technology Improvements

1. **AI-Powered Source Discovery**: Automated new source identification
2. **Real-time Data Streaming**: Live data feeds where available
3. **Advanced Entity Resolution**: ML-based duplicate detection
4. **Predictive Data Quality**: Proactive quality issue detection
5. **Automated Legal Compliance**: Dynamic ToS monitoring

### API Integration Roadmap

1. **Q1**: ACRA API integration
2. **Q2**: LinkedIn Marketing API
3. **Q3**: Crunchbase Enterprise API
4. **Q4**: Custom data partnerships

---

## Conclusion

The Singapore Company Database leverages a diverse ecosystem of 12+ data sources to create comprehensive company profiles. The multi-source approach ensures high data coverage while maintaining quality through validation and deduplication processes.

Key success factors:
- **Strategic Source Selection**: Balance of official, commercial, and social sources
- **Legal Compliance**: Proactive adherence to terms of service and data protection laws
- **Technical Robustness**: Resilient extraction with proper rate limiting and error handling
- **Quality Assurance**: Multi-layered validation and source reliability scoring
- **Continuous Monitoring**: Ongoing assessment of source quality and availability

This foundation supports the creation of a high-quality, comprehensive database of Singapore companies that meets the assessment requirements while maintaining ethical and legal standards.
