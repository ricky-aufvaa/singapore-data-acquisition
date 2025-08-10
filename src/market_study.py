"""
Market Study Module for Singapore Company Database
Analyzes available data sources and provides insights for data acquisition strategy
"""

import asyncio
import aiohttp
import requests
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json
import time
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import pandas as pd

from src.config import settings
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class DataSource:
    """Data source information"""
    name: str
    url: str
    source_type: str  # official, directory, social, financial, procurement
    access_method: str  # api, scraping, manual, hybrid
    estimated_coverage: int
    data_types: List[str]
    reliability_score: float
    legal_status: str  # compliant, requires_review, restricted
    robots_txt_compliant: bool = True
    rate_limits: Optional[str] = None
    api_key_required: bool = False
    cost: str = "Free"
    last_checked: datetime = field(default_factory=datetime.now)
    notes: str = ""


class MarketStudyAnalyzer:
    """Analyzes Singapore company data landscape"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': settings.scraping.user_agent
        })
        self.data_sources: List[DataSource] = []
        
    async def conduct_market_study(self) -> Dict:
        """Conduct comprehensive market study"""
        logger.info("Starting Singapore company data market study")
        
        # Initialize data sources
        await self._initialize_data_sources()
        
        # Analyze each source
        analysis_results = {}
        for source in self.data_sources:
            logger.info(f"Analyzing data source: {source.name}")
            analysis = await self._analyze_data_source(source)
            analysis_results[source.name] = analysis
            
        # Generate market insights
        market_insights = self._generate_market_insights()
        
        # Create comprehensive report
        report = {
            'study_date': datetime.now().isoformat(),
            'total_sources_analyzed': len(self.data_sources),
            'data_sources': [self._source_to_dict(source) for source in self.data_sources],
            'source_analysis': analysis_results,
            'market_insights': market_insights,
            'recommendations': self._generate_recommendations(),
            'estimated_total_coverage': self._estimate_total_coverage()
        }
        
        # Save report
        await self._save_market_study_report(report)
        
        logger.info("Market study completed successfully")
        return report
    
    async def _initialize_data_sources(self):
        """Initialize comprehensive list of data sources"""
        
        # Official Government Sources
        self.data_sources.extend([
            DataSource(
                name="ACRA Business Registry",
                url="https://www.acra.gov.sg",
                source_type="official",
                access_method="hybrid",
                estimated_coverage=500000,
                data_types=["uen", "company_name", "registration_date", "status", "industry"],
                reliability_score=0.95,
                legal_status="compliant",
                notes="Primary source for UEN and official company data"
            ),
            DataSource(
                name="SGX Listed Companies",
                url="https://www.sgx.com",
                source_type="financial",
                access_method="api",
                estimated_coverage=700,
                data_types=["stock_code", "market_cap", "financial_data", "annual_reports"],
                reliability_score=0.90,
                legal_status="compliant",
                api_key_required=True
            ),
            DataSource(
                name="GeBIZ Government Tenders",
                url="https://www.gebiz.gov.sg",
                source_type="procurement",
                access_method="scraping",
                estimated_coverage=10000,
                data_types=["vendor_info", "contract_awards", "business_capability"],
                reliability_score=0.80,
                legal_status="compliant"
            )
        ])
        
        # Business Directories
        self.data_sources.extend([
            DataSource(
                name="Yellow Pages Singapore",
                url="https://www.yellowpages.com.sg",
                source_type="directory",
                access_method="scraping",
                estimated_coverage=100000,
                data_types=["contact_info", "address", "category", "website"],
                reliability_score=0.75,
                legal_status="requires_review",
                rate_limits="Moderate rate limiting required"
            ),
            DataSource(
                name="Singapore Business Directory",
                url="https://www.sgbusinessdirectory.com",
                source_type="directory",
                access_method="scraping",
                estimated_coverage=50000,
                data_types=["company_profiles", "contact_details", "services"],
                reliability_score=0.70,
                legal_status="requires_review"
            ),
            DataSource(
                name="Kompass Singapore",
                url="https://sg.kompass.com",
                source_type="directory",
                access_method="scraping",
                estimated_coverage=30000,
                data_types=["company_details", "products", "trade_data"],
                reliability_score=0.75,
                legal_status="requires_review"
            )
        ])
        
        # Social Media and Professional Networks
        self.data_sources.extend([
            DataSource(
                name="LinkedIn Company Pages",
                url="https://www.linkedin.com",
                source_type="social",
                access_method="api",
                estimated_coverage=50000,
                data_types=["employee_count", "industry", "company_size", "updates"],
                reliability_score=0.85,
                legal_status="requires_review",
                api_key_required=True,
                rate_limits="Strict API limits"
            ),
            DataSource(
                name="Facebook Business Pages",
                url="https://www.facebook.com",
                source_type="social",
                access_method="api",
                estimated_coverage=25000,
                data_types=["page_info", "contact_details", "reviews"],
                reliability_score=0.70,
                legal_status="requires_review",
                api_key_required=True
            )
        ])
        
        # Startup and Investment Databases
        self.data_sources.extend([
            DataSource(
                name="Crunchbase",
                url="https://www.crunchbase.com",
                source_type="financial",
                access_method="api",
                estimated_coverage=5000,
                data_types=["funding_data", "investor_info", "startup_profiles"],
                reliability_score=0.85,
                legal_status="compliant",
                api_key_required=True,
                cost="Paid API"
            ),
            DataSource(
                name="AngelList (Wellfound)",
                url="https://wellfound.com",
                source_type="financial",
                access_method="scraping",
                estimated_coverage=3000,
                data_types=["startup_jobs", "company_profiles", "funding_status"],
                reliability_score=0.75,
                legal_status="requires_review"
            )
        ])
        
        # Industry-Specific Sources
        self.data_sources.extend([
            DataSource(
                name="FinTech Singapore Directory",
                url="https://fintechsingapore.org",
                source_type="directory",
                access_method="scraping",
                estimated_coverage=500,
                data_types=["fintech_companies", "services", "partnerships"],
                reliability_score=0.80,
                legal_status="compliant"
            ),
            DataSource(
                name="Singapore Manufacturing Federation",
                url="https://www.smfederation.org.sg",
                source_type="directory",
                access_method="scraping",
                estimated_coverage=2000,
                data_types=["manufacturing_companies", "capabilities", "certifications"],
                reliability_score=0.80,
                legal_status="compliant"
            )
        ])
        
        # Company Websites (Dynamic)
        self.data_sources.append(
            DataSource(
                name="Company Websites",
                url="Various",
                source_type="primary",
                access_method="scraping",
                estimated_coverage=200000,
                data_types=["about_us", "products", "services", "contact", "team_size"],
                reliability_score=0.75,
                legal_status="requires_review",
                notes="Individual website scraping based on discovered URLs"
            )
        )
    
    async def _analyze_data_source(self, source: DataSource) -> Dict:
        """Analyze individual data source"""
        analysis = {
            'accessibility': 'unknown',
            'robots_txt_status': 'unknown',
            'response_time': None,
            'content_structure': 'unknown',
            'estimated_records': source.estimated_coverage,
            'data_freshness': 'unknown',
            'technical_challenges': [],
            'opportunities': []
        }
        
        try:
            # Check robots.txt if applicable
            if source.access_method in ['scraping', 'hybrid']:
                robots_status = await self._check_robots_txt(source.url)
                analysis['robots_txt_status'] = robots_status
                source.robots_txt_compliant = robots_status == 'compliant'
            
            # Test accessibility
            start_time = time.time()
            response = await self._test_url_accessibility(source.url)
            analysis['response_time'] = time.time() - start_time
            
            if response:
                analysis['accessibility'] = 'accessible'
                
                # Analyze content structure for scraping sources
                if source.access_method in ['scraping', 'hybrid']:
                    structure_analysis = await self._analyze_content_structure(source.url, response)
                    analysis['content_structure'] = structure_analysis
                
            else:
                analysis['accessibility'] = 'inaccessible'
                analysis['technical_challenges'].append('URL not accessible')
                
        except Exception as e:
            logger.warning(f"Error analyzing {source.name}: {str(e)}")
            analysis['technical_challenges'].append(f"Analysis error: {str(e)}")
        
        # Add source-specific insights
        analysis.update(self._get_source_specific_insights(source))
        
        return analysis
    
    async def _check_robots_txt(self, base_url: str) -> str:
        """Check robots.txt compliance"""
        try:
            robots_url = urljoin(base_url, '/robots.txt')
            response = self.session.get(robots_url, timeout=10)
            
            if response.status_code == 200:
                robots_content = response.text.lower()
                
                # Check for restrictive rules
                if 'disallow: /' in robots_content and 'user-agent: *' in robots_content:
                    return 'restricted'
                elif 'crawl-delay' in robots_content:
                    return 'rate_limited'
                else:
                    return 'compliant'
            else:
                return 'no_robots_txt'
                
        except Exception:
            return 'unknown'
    
    async def _test_url_accessibility(self, url: str) -> Optional[requests.Response]:
        """Test if URL is accessible"""
        try:
            response = self.session.get(url, timeout=15, allow_redirects=True)
            return response if response.status_code == 200 else None
        except Exception:
            return None
    
    async def _analyze_content_structure(self, url: str, response: requests.Response) -> Dict:
        """Analyze webpage content structure"""
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            structure = {
                'has_search_functionality': bool(soup.find('input', {'type': 'search'}) or 
                                               soup.find('form', class_=lambda x: x and 'search' in x.lower())),
                'pagination_detected': bool(soup.find(class_=lambda x: x and 'pag' in x.lower()) or
                                          soup.find('a', string=lambda x: x and 'next' in x.lower())),
                'company_listings_detected': len(soup.find_all(class_=lambda x: x and any(
                    term in x.lower() for term in ['company', 'business', 'listing', 'directory']
                ))),
                'contact_info_patterns': len(soup.find_all(string=lambda x: x and any(
                    pattern in x for pattern in ['@', '+65', 'singapore']
                ))),
                'structured_data': bool(soup.find('script', {'type': 'application/ld+json'})),
                'estimated_scrapability': 'unknown'
            }
            
            # Estimate scrapability
            scrapability_score = 0
            if structure['has_search_functionality']:
                scrapability_score += 2
            if structure['pagination_detected']:
                scrapability_score += 2
            if structure['company_listings_detected'] > 5:
                scrapability_score += 3
            if structure['structured_data']:
                scrapability_score += 2
                
            if scrapability_score >= 7:
                structure['estimated_scrapability'] = 'high'
            elif scrapability_score >= 4:
                structure['estimated_scrapability'] = 'medium'
            else:
                structure['estimated_scrapability'] = 'low'
                
            return structure
            
        except Exception as e:
            return {'error': str(e), 'estimated_scrapability': 'unknown'}
    
    def _get_source_specific_insights(self, source: DataSource) -> Dict:
        """Get insights specific to each data source"""
        insights = {
            'opportunities': [],
            'challenges': [],
            'recommended_approach': '',
            'priority_level': 'medium'
        }
        
        # ACRA-specific insights
        if 'acra' in source.name.lower():
            insights['opportunities'].extend([
                'Official UEN registry - highest data quality',
                'Comprehensive coverage of all registered entities',
                'Regular updates and reliable data'
            ])
            insights['challenges'].extend([
                'May require manual search or API access',
                'Rate limiting on bulk queries'
            ])
            insights['recommended_approach'] = 'Hybrid: API where available, targeted scraping for bulk data'
            insights['priority_level'] = 'high'
        
        # LinkedIn-specific insights
        elif 'linkedin' in source.name.lower():
            insights['opportunities'].extend([
                'Rich employee and company size data',
                'Industry classifications',
                'Company updates and news'
            ])
            insights['challenges'].extend([
                'Strict API rate limits',
                'Requires authentication',
                'Anti-scraping measures'
            ])
            insights['recommended_approach'] = 'Official API with careful rate limit management'
            insights['priority_level'] = 'high'
        
        # Directory-specific insights
        elif source.source_type == 'directory':
            insights['opportunities'].extend([
                'Large volume of company listings',
                'Contact information available',
                'Industry categorization'
            ])
            insights['challenges'].extend([
                'Data quality varies',
                'May have duplicate entries',
                'Requires respectful scraping'
            ])
            insights['recommended_approach'] = 'Systematic scraping with deduplication'
            insights['priority_level'] = 'medium'
        
        return insights
    
    def _generate_market_insights(self) -> Dict:
        """Generate comprehensive market insights"""
        total_coverage = sum(source.estimated_coverage for source in self.data_sources)
        
        # Source type distribution
        source_types = {}
        for source in self.data_sources:
            source_types[source.source_type] = source_types.get(source.source_type, 0) + 1
        
        # Access method distribution
        access_methods = {}
        for source in self.data_sources:
            access_methods[source.access_method] = access_methods.get(source.access_method, 0) + 1
        
        # Reliability analysis
        high_reliability_sources = [s for s in self.data_sources if s.reliability_score >= 0.8]
        
        insights = {
            'total_estimated_coverage': total_coverage,
            'unique_companies_estimate': int(total_coverage * 0.3),  # Accounting for overlaps
            'source_type_distribution': source_types,
            'access_method_distribution': access_methods,
            'high_reliability_sources': len(high_reliability_sources),
            'api_sources_available': len([s for s in self.data_sources if s.api_key_required]),
            'free_sources': len([s for s in self.data_sources if s.cost == "Free"]),
            'data_coverage_analysis': self._analyze_data_coverage(),
            'technical_feasibility': self._assess_technical_feasibility(),
            'legal_compliance_status': self._assess_legal_compliance()
        }
        
        return insights
    
    def _analyze_data_coverage(self) -> Dict:
        """Analyze what data fields can be covered by available sources"""
        field_coverage = {
            'uen': ['ACRA Business Registry'],
            'company_name': ['ACRA Business Registry', 'Yellow Pages Singapore', 'LinkedIn Company Pages'],
            'website': ['Yellow Pages Singapore', 'Company Websites'],
            'industry': ['ACRA Business Registry', 'LinkedIn Company Pages'],
            'employee_count': ['LinkedIn Company Pages'],
            'contact_info': ['Yellow Pages Singapore', 'Company Websites'],
            'financial_data': ['SGX Listed Companies', 'Crunchbase'],
            'social_media': ['LinkedIn Company Pages', 'Facebook Business Pages'],
            'products_services': ['Company Websites', 'Business Directories']
        }
        
        coverage_stats = {}
        for field, sources in field_coverage.items():
            total_coverage = sum(
                source.estimated_coverage for source in self.data_sources 
                if source.name in sources
            )
            coverage_stats[field] = {
                'sources': len(sources),
                'estimated_coverage': total_coverage,
                'primary_source': sources[0] if sources else None
            }
        
        return coverage_stats
    
    def _assess_technical_feasibility(self) -> Dict:
        """Assess technical feasibility of data extraction"""
        return {
            'scraping_complexity': 'Medium - Mix of simple and complex sites',
            'api_integration_required': True,
            'rate_limiting_challenges': 'Moderate - Most sources require respectful scraping',
            'anti_bot_measures': 'Present on major platforms (LinkedIn, Facebook)',
            'estimated_development_time': '4-6 weeks for full pipeline',
            'infrastructure_requirements': 'Moderate - Proxy rotation, caching, monitoring'
        }
    
    def _assess_legal_compliance(self) -> Dict:
        """Assess legal compliance status"""
        compliant_sources = len([s for s in self.data_sources if s.legal_status == 'compliant'])
        review_required = len([s for s in self.data_sources if s.legal_status == 'requires_review'])
        
        return {
            'compliant_sources': compliant_sources,
            'sources_requiring_review': review_required,
            'robots_txt_compliant': len([s for s in self.data_sources if s.robots_txt_compliant]),
            'terms_of_service_review_needed': True,
            'data_protection_considerations': [
                'PDPA compliance for personal data',
                'Respect for website terms of service',
                'Rate limiting to avoid service disruption'
            ]
        }
    
    def _generate_recommendations(self) -> Dict:
        """Generate strategic recommendations"""
        return {
            'primary_strategy': 'Multi-source approach with ACRA as primary UEN source',
            'recommended_extraction_order': [
                '1. ACRA Business Registry - Get UEN list and basic company data',
                '2. Company website discovery through directories',
                '3. Website scraping for detailed company information',
                '4. Social media enrichment (LinkedIn, Facebook)',
                '5. Financial data from SGX and Crunchbase',
                '6. LLM processing for data enrichment'
            ],
            'technical_recommendations': [
                'Implement robust rate limiting and retry mechanisms',
                'Use proxy rotation for large-scale scraping',
                'Implement comprehensive caching to avoid re-scraping',
                'Build modular extractors for each data source',
                'Implement data quality validation at each step'
            ],
            'legal_recommendations': [
                'Review terms of service for each platform',
                'Implement robots.txt compliance checking',
                'Add user-agent identification and contact information',
                'Implement respectful crawling delays',
                'Consider reaching out to platforms for partnership opportunities'
            ],
            'data_quality_recommendations': [
                'Use ACRA UEN as primary key for deduplication',
                'Implement fuzzy matching for company names',
                'Cross-validate data across multiple sources',
                'Implement confidence scoring for each data point',
                'Regular data freshness checks and updates'
            ]
        }
    
    def _estimate_total_coverage(self) -> Dict:
        """Estimate total achievable coverage"""
        # Conservative estimates accounting for overlaps and accessibility
        base_coverage = 50000  # ACRA accessible companies
        directory_addition = 30000  # Additional from directories
        website_enrichment = 40000  # Companies with scrapable websites
        
        return {
            'conservative_estimate': base_coverage,
            'realistic_estimate': base_coverage + directory_addition,
            'optimistic_estimate': base_coverage + directory_addition + website_enrichment,
            'target_for_assessment': 50000,
            'confidence_level': 'High - Multiple reliable sources identified'
        }
    
    def _source_to_dict(self, source: DataSource) -> Dict:
        """Convert DataSource to dictionary"""
        return {
            'name': source.name,
            'url': source.url,
            'source_type': source.source_type,
            'access_method': source.access_method,
            'estimated_coverage': source.estimated_coverage,
            'data_types': source.data_types,
            'reliability_score': source.reliability_score,
            'legal_status': source.legal_status,
            'robots_txt_compliant': source.robots_txt_compliant,
            'rate_limits': source.rate_limits,
            'api_key_required': source.api_key_required,
            'cost': source.cost,
            'notes': source.notes
        }
    
    async def _save_market_study_report(self, report: Dict):
        """Save market study report to file"""
        import os
        os.makedirs('data/reports', exist_ok=True)
        
        # Save as JSON
        with open('data/reports/market_study_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Save as CSV for data sources
        df = pd.DataFrame([source for source in report['data_sources']])
        df.to_csv('data/reports/data_sources_analysis.csv', index=False)
        
        logger.info("Market study report saved to data/reports/")


async def main():
    """Run market study analysis"""
    analyzer = MarketStudyAnalyzer()
    report = await analyzer.conduct_market_study()
    
    print("\n" + "="*60)
    print("SINGAPORE COMPANY DATA MARKET STUDY COMPLETED")
    print("="*60)
    print(f"Total Sources Analyzed: {report['total_sources_analyzed']}")
    print(f"Estimated Total Coverage: {report['estimated_total_coverage']['realistic_estimate']:,} companies")
    print(f"High Reliability Sources: {report['market_insights']['high_reliability_sources']}")
    print(f"API Sources Available: {report['market_insights']['api_sources_available']}")
    print("\nDetailed report saved to: data/reports/market_study_report.json")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
