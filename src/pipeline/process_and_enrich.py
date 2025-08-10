"""
Data Processing and Enrichment Module for Singapore Company Database
Handles data cleaning, normalization, entity matching, and LLM enrichment
"""

import asyncio
import re
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import pandas as pd
from fuzzywuzzy import fuzz, process
from email_validator import validate_email, EmailNotValidError
import phonenumbers
from urllib.parse import urlparse, urljoin
import hashlib

from src.config import settings
from src.utils.logging_config import get_logger, LoggingContext
from src.processors.llm_enricher import llm_enricher
from src.database.connection import db_manager

logger = get_logger(__name__)


class DataProcessor:
    """Main class for processing and enriching company data"""
    
    def __init__(self):
        self.processed_companies = []
        self.duplicate_matches = []
        self.quality_issues = []
        
    async def clean_and_normalize(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and normalize company data"""
        cleaned_companies = []
        
        with LoggingContext(logger, "Data Cleaning and Normalization"):
            for i, company in enumerate(companies):
                try:
                    cleaned_company = await self._clean_single_company(company)
                    if cleaned_company:
                        cleaned_companies.append(cleaned_company)
                    
                    if i % 1000 == 0:
                        logger.info(f"Cleaned {i}/{len(companies)} companies")
                        
                except Exception as e:
                    logger.error(f"Error cleaning company {company.get('company_name', 'Unknown')}: {e}")
                    self.quality_issues.append({
                        'company': company.get('company_name', 'Unknown'),
                        'issue': f"Cleaning error: {str(e)}",
                        'severity': 'error'
                    })
            
            logger.info(f"Cleaned {len(cleaned_companies)} companies from {len(companies)} input records")
            
        return cleaned_companies
    
    async def _clean_single_company(self, company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clean and normalize a single company record"""
        cleaned = company.copy()
        
        # Clean company name
        cleaned['company_name'] = self._clean_company_name(company.get('company_name', ''))
        if not cleaned['company_name']:
            return None  # Skip companies without valid names
        
        # Generate normalized name for matching
        cleaned['company_name_normalized'] = self._normalize_company_name(cleaned['company_name'])
        
        # Clean and validate UEN
        if 'uen' in company:
            cleaned['uen'] = self._clean_uen(company['uen'])
        
        # Clean and validate website
        if 'website' in company:
            cleaned['website'] = self._clean_website_url(company['website'])
        
        # Clean contact information
        if 'contact_email' in company:
            cleaned['contact_email'] = self._clean_email(company['contact_email'])
        
        if 'contact_phone' in company:
            cleaned['contact_phone'] = self._clean_phone_number(company['contact_phone'])
        
        # Normalize industry
        if 'industry' in company:
            cleaned['industry'] = self._normalize_industry(company['industry'])
        
        # Normalize company size
        if 'company_size' in company:
            cleaned['company_size'] = self._normalize_company_size(company['company_size'])
        
        # Clean social media URLs
        for social_field in ['linkedin', 'facebook', 'instagram']:
            if social_field in company:
                cleaned[social_field] = self._clean_social_media_url(company[social_field], social_field)
        
        # Clean financial data
        if 'revenue' in company:
            cleaned['revenue'] = self._clean_revenue(company['revenue'])
        
        if 'founding_year' in company:
            cleaned['founding_year'] = self._clean_founding_year(company['founding_year'])
        
        # Clean employee count
        if 'number_of_employees' in company:
            cleaned['number_of_employees'] = self._clean_employee_count(company['number_of_employees'])
        
        # Clean arrays
        for array_field in ['keywords', 'products_offered', 'services_offered']:
            if array_field in company:
                cleaned[array_field] = self._clean_array_field(company[array_field])
        
        # Set default values
        cleaned['hq_country'] = cleaned.get('hq_country', 'Singapore')
        cleaned['no_of_locations_in_singapore'] = cleaned.get('no_of_locations_in_singapore', 1)
        
        # Add processing metadata
        cleaned['processed_at'] = datetime.now().isoformat()
        cleaned['data_quality_score'] = self._calculate_initial_quality_score(cleaned)
        
        return cleaned
    
    def _clean_company_name(self, name: str) -> str:
        """Clean and standardize company name"""
        if not name or not isinstance(name, str):
            return ""
        
        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', name.strip())
        
        # Remove common prefixes/suffixes that might cause duplicates
        # But keep them for official records
        
        # Validate length
        if len(name) < settings.data_quality.min_company_name_length:
            return ""
        if len(name) > settings.data_quality.max_company_name_length:
            name = name[:settings.data_quality.max_company_name_length]
        
        return name
    
    def _normalize_company_name(self, name: str) -> str:
        """Create normalized version for fuzzy matching"""
        if not name:
            return ""
        
        # Convert to lowercase
        normalized = name.lower()
        
        # Remove common business suffixes for matching
        suffixes = [
            'pte ltd', 'private limited', 'pvt ltd', 'ltd', 'limited',
            'inc', 'incorporated', 'corp', 'corporation', 'llc',
            'sdn bhd', 'bhd', 'co', 'company'
        ]
        
        for suffix in suffixes:
            if normalized.endswith(' ' + suffix):
                normalized = normalized[:-len(suffix)-1].strip()
        
        # Remove special characters
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Remove extra whitespace
        normalized = re.sub(r'\s+', ' ', normalized.strip())
        
        return normalized
    
    def _clean_uen(self, uen: str) -> str:
        """Clean and validate Singapore UEN"""
        if not uen or not isinstance(uen, str):
            return ""
        
        # Remove whitespace and convert to uppercase
        uen = uen.strip().upper()
        
        # Singapore UEN format: YYYYNNNNNX (where Y=year, N=number, X=suffix)
        uen_pattern = r'^(19|20)\d{2}\d{6}[A-Z]$'
        
        if re.match(uen_pattern, uen):
            return uen
        
        # Try to extract UEN from longer strings
        uen_match = re.search(r'((?:19|20)\d{2}\d{6}[A-Z])', uen)
        if uen_match:
            return uen_match.group(1)
        
        return ""
    
    def _clean_website_url(self, url: str) -> str:
        """Clean and validate website URL"""
        if not url or not isinstance(url, str):
            return ""
        
        url = url.strip()
        
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            parsed = urlparse(url)
            if parsed.netloc:
                # Normalize URL
                normalized_url = f"{parsed.scheme}://{parsed.netloc.lower()}"
                if parsed.path and parsed.path != '/':
                    normalized_url += parsed.path
                return normalized_url
        except Exception:
            pass
        
        return ""
    
    def _clean_email(self, email: str) -> str:
        """Clean and validate email address"""
        if not email or not isinstance(email, str):
            return ""
        
        email = email.strip().lower()
        
        try:
            validated_email = validate_email(email)
            return validated_email.email
        except EmailNotValidError:
            return ""
    
    def _clean_phone_number(self, phone: str) -> str:
        """Clean and validate phone number (Singapore format)"""
        if not phone or not isinstance(phone, str):
            return ""
        
        try:
            # Parse with Singapore as default region
            parsed_number = phonenumbers.parse(phone, "SG")
            
            if phonenumbers.is_valid_number(parsed_number):
                # Format in international format
                return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        except Exception:
            pass
        
        return ""
    
    def _normalize_industry(self, industry: str) -> str:
        """Normalize industry classification"""
        if not industry or not isinstance(industry, str):
            return ""
        
        industry = industry.strip().title()
        
        # Map common variations to standard industries
        industry_mapping = {
            'Information Technology': 'Technology',
            'IT': 'Technology',
            'Software': 'Technology',
            'Fintech': 'FinTech',
            'Financial Technology': 'FinTech',
            'Banking': 'FinTech',
            'Finance': 'FinTech',
            'Food & Beverage': 'F&B',
            'Food And Beverage': 'F&B',
            'Restaurant': 'F&B',
            'Professional Service': 'Professional Services',
            'Consulting': 'Professional Services',
            'Real Estate': 'Real Estate',
            'Property': 'Real Estate',
            'Healthcare': 'Healthcare',
            'Medical': 'Healthcare',
            'E-Commerce': 'E-commerce',
            'Ecommerce': 'E-commerce',
            'Online Retail': 'E-commerce'
        }
        
        # Check for exact matches first
        if industry in settings.industries:
            return industry
        
        # Check mappings
        if industry in industry_mapping:
            return industry_mapping[industry]
        
        # Fuzzy match against valid industries
        best_match = process.extractOne(industry, settings.industries, scorer=fuzz.ratio)
        if best_match and best_match[1] >= 80:  # 80% similarity threshold
            return best_match[0]
        
        return "Other"
    
    def _normalize_company_size(self, size: str) -> str:
        """Normalize company size classification"""
        if not size or not isinstance(size, str):
            return "Unknown"
        
        size = size.strip()
        
        # Check for exact matches
        if size in settings.company_sizes:
            return size
        
        # Extract numbers and classify
        numbers = re.findall(r'\d+', size)
        if numbers:
            max_employees = max(int(num) for num in numbers)
            
            if max_employees <= 10:
                return "Micro (1-10)"
            elif max_employees <= 50:
                return "Small (11-50)"
            elif max_employees <= 200:
                return "Medium (51-200)"
            elif max_employees <= 1000:
                return "Large (201-1000)"
            else:
                return "Enterprise (1000+)"
        
        # Fuzzy match against valid sizes
        best_match = process.extractOne(size, settings.company_sizes, scorer=fuzz.ratio)
        if best_match and best_match[1] >= 70:
            return best_match[0]
        
        return "Unknown"
    
    def _clean_social_media_url(self, url: str, platform: str) -> str:
        """Clean and validate social media URL"""
        if not url or not isinstance(url, str):
            return ""
        
        url = url.strip()
        
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            parsed = urlparse(url)
            
            # Validate domain
            valid_domains = {
                'linkedin': ['linkedin.com', 'www.linkedin.com'],
                'facebook': ['facebook.com', 'www.facebook.com', 'fb.com'],
                'instagram': ['instagram.com', 'www.instagram.com']
            }
            
            if platform in valid_domains and parsed.netloc.lower() in valid_domains[platform]:
                return url
        except Exception:
            pass
        
        return ""
    
    def _clean_revenue(self, revenue: Any) -> Optional[float]:
        """Clean and validate revenue data"""
        if revenue is None:
            return None
        
        if isinstance(revenue, (int, float)):
            return float(revenue) if revenue >= 0 else None
        
        if isinstance(revenue, str):
            # Remove currency symbols and commas
            revenue_str = re.sub(r'[^\d.]', '', revenue)
            try:
                return float(revenue_str) if revenue_str else None
            except ValueError:
                return None
        
        return None
    
    def _clean_founding_year(self, year: Any) -> Optional[int]:
        """Clean and validate founding year"""
        if year is None:
            return None
        
        if isinstance(year, int):
            if 1800 <= year <= datetime.now().year:
                return year
        
        if isinstance(year, str):
            # Extract year from string
            year_match = re.search(r'(19|20)\d{2}', year)
            if year_match:
                year_int = int(year_match.group())
                if 1800 <= year_int <= datetime.now().year:
                    return year_int
        
        return None
    
    def _clean_employee_count(self, count: Any) -> Optional[int]:
        """Clean and validate employee count"""
        if count is None:
            return None
        
        if isinstance(count, int):
            return count if count >= 0 else None
        
        if isinstance(count, str):
            # Extract number from string
            numbers = re.findall(r'\d+', count)
            if numbers:
                return int(numbers[0])
        
        return None
    
    def _clean_array_field(self, field: Any) -> List[str]:
        """Clean array fields like keywords, products, services"""
        if not field:
            return []
        
        if isinstance(field, str):
            # Split by common delimiters
            items = re.split(r'[,;|]', field)
        elif isinstance(field, list):
            items = field
        else:
            return []
        
        # Clean each item
        cleaned_items = []
        for item in items:
            if isinstance(item, str):
                item = item.strip()
                if item and len(item) > 2:  # Minimum length
                    cleaned_items.append(item)
        
        return cleaned_items[:10]  # Limit to 10 items
    
    def _calculate_initial_quality_score(self, company: Dict[str, Any]) -> float:
        """Calculate initial data quality score"""
        total_fields = 20  # Total number of important fields
        filled_fields = 0
        
        # Core fields (higher weight)
        core_fields = ['uen', 'company_name', 'website', 'industry']
        for field in core_fields:
            if company.get(field):
                filled_fields += 2  # Double weight for core fields
        
        # Standard fields
        standard_fields = [
            'contact_email', 'contact_phone', 'linkedin', 'number_of_employees',
            'company_size', 'founding_year', 'keywords'
        ]
        for field in standard_fields:
            if company.get(field):
                filled_fields += 1
        
        # Calculate score (0.0 to 1.0)
        max_possible_score = len(core_fields) * 2 + len(standard_fields)
        score = filled_fields / max_possible_score
        
        return round(min(1.0, score), 2)
    
    async def deduplicate_companies(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate companies using entity matching"""
        with LoggingContext(logger, "Entity Matching and Deduplication"):
            # Create UEN index for exact matches
            uen_index = {}
            name_index = {}
            website_index = {}
            
            unique_companies = []
            duplicates_found = 0
            
            for company in companies:
                is_duplicate = False
                primary_match = None
                
                # Check for UEN duplicates (highest priority)
                uen = company.get('uen')
                if uen and uen in uen_index:
                    is_duplicate = True
                    primary_match = uen_index[uen]
                    match_type = 'uen_exact'
                
                # Check for website duplicates
                elif not is_duplicate:
                    website = company.get('website')
                    if website and website in website_index:
                        is_duplicate = True
                        primary_match = website_index[website]
                        match_type = 'website_exact'
                
                # Check for fuzzy name matches
                elif not is_duplicate:
                    normalized_name = company.get('company_name_normalized', '')
                    if normalized_name:
                        best_match = self._find_fuzzy_name_match(normalized_name, name_index)
                        if best_match:
                            is_duplicate = True
                            primary_match = best_match['company']
                            match_type = 'name_fuzzy'
                
                if is_duplicate and primary_match:
                    # Merge data into primary record
                    merged_company = self._merge_company_records(primary_match, company)
                    
                    # Update indexes
                    if uen:
                        uen_index[uen] = merged_company
                    if website:
                        website_index[website] = merged_company
                    if normalized_name:
                        name_index[normalized_name] = merged_company
                    
                    # Record the duplicate match
                    self.duplicate_matches.append({
                        'primary_uen': merged_company.get('uen'),
                        'duplicate_uen': company.get('uen'),
                        'match_type': match_type,
                        'match_score': best_match.get('score', 1.0) if 'best_match' in locals() else 1.0
                    })
                    
                    duplicates_found += 1
                
                else:
                    # Add as new unique company
                    unique_companies.append(company)
                    
                    # Update indexes
                    if uen:
                        uen_index[uen] = company
                    if website:
                        website_index[website] = company
                    if normalized_name:
                        name_index[normalized_name] = company
            
            logger.info(f"Deduplication complete: {len(companies)} -> {len(unique_companies)} companies ({duplicates_found} duplicates removed)")
            
        return unique_companies
    
    def _find_fuzzy_name_match(self, normalized_name: str, name_index: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find fuzzy match for company name"""
        if not normalized_name or len(normalized_name) < 3:
            return None
        
        best_match = None
        best_score = 0
        
        for existing_name, company in name_index.items():
            score = fuzz.ratio(normalized_name, existing_name)
            
            if score >= settings.data_quality.fuzzy_match_threshold and score > best_score:
                best_score = score
                best_match = {
                    'company': company,
                    'score': score / 100.0
                }
        
        return best_match
    
    def _merge_company_records(self, primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two company records, preferring primary but filling gaps with secondary"""
        merged = primary.copy()
        
        # Merge fields, preferring non-empty values
        for key, value in secondary.items():
            if key not in merged or not merged[key]:
                merged[key] = value
            elif key in ['keywords', 'products_offered', 'services_offered'] and isinstance(value, list):
                # Merge arrays
                existing_items = set(merged.get(key, []))
                for item in value:
                    if item not in existing_items:
                        merged.setdefault(key, []).append(item)
        
        # Update quality score
        merged['data_quality_score'] = self._calculate_initial_quality_score(merged)
        
        # Track merge in source data
        primary_sources = merged.get('source_of_data', '')
        secondary_sources = secondary.get('source_of_data', '')
        
        if secondary_sources and secondary_sources not in primary_sources:
            merged['source_of_data'] = f"{primary_sources}, {secondary_sources}"
        
        return merged
    
    async def enrich_with_llm(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich companies using LLM processing"""
        enriched_companies = []
        
        with LoggingContext(logger, "LLM Enrichment"):
            batch_size = settings.pipeline.batch_size
            
            for i in range(0, len(companies), batch_size):
                batch = companies[i:i + batch_size]
                
                try:
                    enriched_batch = await llm_enricher.enrich_company_batch(batch)
                    enriched_companies.extend(enriched_batch)
                    
                    logger.info(f"LLM enriched batch {i//batch_size + 1}/{(len(companies) + batch_size - 1)//batch_size}")
                    
                except Exception as e:
                    logger.error(f"Error in LLM enrichment batch {i//batch_size + 1}: {e}")
                    # Add original batch without enrichment
                    enriched_companies.extend(batch)
            
            logger.info(f"LLM enrichment complete: {len(enriched_companies)} companies processed")
        
        return enriched_companies
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return {
            'total_processed': len(self.processed_companies),
            'duplicates_found': len(self.duplicate_matches),
            'quality_issues': len(self.quality_issues),
            'duplicate_breakdown': self._get_duplicate_breakdown(),
            'quality_issue_breakdown': self._get_quality_issue_breakdown()
        }
    
    def _get_duplicate_breakdown(self) -> Dict[str, int]:
        """Get breakdown of duplicate types"""
        breakdown = {}
        for match in self.duplicate_matches:
            match_type = match.get('match_type', 'unknown')
            breakdown[match_type] = breakdown.get(match_type, 0) + 1
        return breakdown
    
    def _get_quality_issue_breakdown(self) -> Dict[str, int]:
        """Get breakdown of quality issues"""
        breakdown = {}
        for issue in self.quality_issues:
            severity = issue.get('severity', 'unknown')
            breakdown[severity] = breakdown.get(severity, 0) + 1
        return breakdown


# Example usage and testing
if __name__ == "__main__":
    async def test_data_processing():
        """Test data processing functionality"""
        processor = DataProcessor()
        
        # Sample test data
        test_companies = [
            {
                'company_name': 'Test Company Pte Ltd',
                'uen': '200012345A',
                'website': 'www.testcompany.com.sg',
                'contact_email': 'info@testcompany.com.sg',
                'contact_phone': '6512345678',
                'industry': 'Technology',
                'source_of_data': 'Test Data'
            },
            {
                'company_name': 'Test Company Private Limited',  # Duplicate
                'website': 'https://www.testcompany.com.sg',
                'contact_email': 'contact@testcompany.com.sg',
                'industry': 'IT',
                'source_of_data': 'Test Data 2'
            }
        ]
        
        print("Testing data processing...")
        
        # Test cleaning
        cleaned = await processor.clean_and_normalize(test_companies)
        print(f"Cleaned {len(cleaned)} companies")
        
        # Test deduplication
        deduplicated = await processor.deduplicate_companies(cleaned)
        print(f"After deduplication: {len(deduplicated)} companies")
        
        # Show results
        if deduplicated:
            print("\nSample processed company:")
            print(json.dumps(deduplicated[0], indent=2, default=str))
        
        # Show stats
        stats = processor.get_processing_stats()
        print(f"\nProcessing stats: {stats}")
    
    asyncio.run(test_data_processing())
