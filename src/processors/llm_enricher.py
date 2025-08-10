"""
LLM Enricher for Singapore Company Database
Uses Llama 3 via Ollama for intelligent data enrichment and classification
"""

import asyncio
import aiohttp
import json
import time
import re
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import ollama
from concurrent.futures import ThreadPoolExecutor
import threading

from src.config import settings
from src.utils.logging_config import get_logger
from src.database.connection import db_manager

logger = get_logger(__name__)


@dataclass
class LLMResponse:
    """LLM response with metadata"""
    content: str
    confidence_score: float
    processing_time_ms: int
    tokens_used: int
    model_name: str
    prompt_type: str
    error: Optional[str] = None


class PromptTemplates:
    """Collection of prompt templates for different enrichment tasks"""
    
    INDUSTRY_CLASSIFICATION = """
Analyze the following company information and classify it into ONE of these industries:
Technology, FinTech, Healthcare, E-commerce, Manufacturing, Professional Services, Real Estate, F&B, Education, Logistics, Construction, Retail, Energy, Media, Automotive, Agriculture, Tourism, Government, Non-Profit, Other

Company Name: {company_name}
Website Content: {website_content}
Description: {description}

Consider the company's primary business activity, products, and services. Return ONLY the industry name from the list above.

Industry:"""

    KEYWORD_EXTRACTION = """
Extract 5-10 relevant business keywords from the following company information. Focus on:
- Products and services offered
- Technologies used
- Market segments served
- Business model
- Key capabilities

Company Name: {company_name}
Website Content: {website_content}
About Us: {about_content}

Return keywords as a comma-separated list. Be specific and avoid generic terms.

Keywords:"""

    COMPANY_SIZE_ESTIMATION = """
Based on the following company information, estimate the company size category:
Micro (1-10), Small (11-50), Medium (51-200), Large (201-1000), Enterprise (1000+)

Company Name: {company_name}
Website Content: {website_content}
About Us: {about_content}
Team/Career Pages: {team_content}

Look for indicators like:
- Explicit employee count mentions
- Team size descriptions
- Office locations
- Scale of operations
- Language used (e.g., "we are a small team", "our 500+ employees")

Return ONLY the size category from the list above.

Company Size:"""

    PRODUCTS_SERVICES_EXTRACTION = """
Extract the main products and services offered by this company from the provided information.

Company Name: {company_name}
Website Content: {website_content}
Products/Services Pages: {products_content}

Separate products and services clearly. Be specific and avoid marketing language.

Format your response as:
PRODUCTS: [list products separated by semicolons]
SERVICES: [list services separated by semicolons]

If no clear distinction, list everything under SERVICES.

Response:"""

    CONTACT_INFO_EXTRACTION = """
Extract contact information from the following company website content:

Company Name: {company_name}
Website Content: {website_content}
Contact Page: {contact_content}

Look for:
- Email addresses (especially general/info emails)
- Phone numbers (Singapore format preferred)
- Physical addresses

Format your response as:
EMAIL: [email address or "Not found"]
PHONE: [phone number or "Not found"]
ADDRESS: [physical address or "Not found"]

Response:"""

    DATA_QUALITY_ASSESSMENT = """
Assess the quality and completeness of this company data on a scale of 0.0 to 1.0:

Company Data:
- Name: {company_name}
- Website: {website}
- Industry: {industry}
- Employee Count: {employee_count}
- Revenue: {revenue}
- Contact Info: {contact_info}
- Description: {description}

Consider:
- Completeness of information
- Consistency across fields
- Reliability of sources
- Data freshness indicators

Return ONLY a decimal number between 0.0 and 1.0.

Quality Score:"""


class LLMEnricher:
    """LLM-powered data enrichment using Llama 3"""
    
    def __init__(self):
        self.client = None
        self.model_name = settings.llm.model_name
        self.temperature = settings.llm.temperature
        self.max_tokens = settings.llm.max_tokens
        self.timeout = settings.llm.timeout
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Ollama client"""
        try:
            # Test connection to Ollama
            response = ollama.list()
            available_models = [model['name'] for model in response.get('models', [])]
            
            if self.model_name not in available_models:
                logger.warning(f"Model {self.model_name} not found. Available models: {available_models}")
                # Try to pull the model
                logger.info(f"Attempting to pull model {self.model_name}")
                ollama.pull(self.model_name)
            
            self.client = ollama
            logger.info(f"LLM client initialized with model: {self.model_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            self.client = None
    
    def _generate_response(self, prompt: str, prompt_type: str) -> LLMResponse:
        """Generate response from LLM"""
        if not self.client:
            return LLMResponse(
                content="",
                confidence_score=0.0,
                processing_time_ms=0,
                tokens_used=0,
                model_name=self.model_name,
                prompt_type=prompt_type,
                error="LLM client not initialized"
            )
        
        start_time = time.time()
        
        try:
            response = self.client.generate(
                model=self.model_name,
                prompt=prompt,
                options={
                    'temperature': self.temperature,
                    'num_predict': self.max_tokens,
                    'top_p': 0.9,
                    'top_k': 40
                }
            )
            
            processing_time_ms = int((time.time() - start_time) * 1000)
            
            content = response.get('response', '').strip()
            
            # Estimate tokens (rough approximation)
            tokens_used = len(prompt.split()) + len(content.split())
            
            # Calculate confidence score based on response characteristics
            confidence_score = self._calculate_confidence_score(content, prompt_type)
            
            return LLMResponse(
                content=content,
                confidence_score=confidence_score,
                processing_time_ms=processing_time_ms,
                tokens_used=tokens_used,
                model_name=self.model_name,
                prompt_type=prompt_type
            )
            
        except Exception as e:
            processing_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"LLM generation error for {prompt_type}: {e}")
            
            return LLMResponse(
                content="",
                confidence_score=0.0,
                processing_time_ms=processing_time_ms,
                tokens_used=0,
                model_name=self.model_name,
                prompt_type=prompt_type,
                error=str(e)
            )
    
    def _calculate_confidence_score(self, content: str, prompt_type: str) -> float:
        """Calculate confidence score based on response characteristics"""
        if not content:
            return 0.0
        
        base_score = 0.7  # Base confidence for any response
        
        # Adjust based on prompt type
        if prompt_type == "industry_classification":
            # Check if response matches expected industry categories
            industries = settings.industries
            if any(industry.lower() in content.lower() for industry in industries):
                base_score += 0.2
        
        elif prompt_type == "keyword_extraction":
            # Check for comma-separated format and reasonable number of keywords
            keywords = [k.strip() for k in content.split(',')]
            if 3 <= len(keywords) <= 12:
                base_score += 0.2
        
        elif prompt_type == "company_size_estimation":
            # Check if response matches expected size categories
            sizes = settings.company_sizes
            if any(size.lower() in content.lower() for size in sizes):
                base_score += 0.2
        
        # Penalize very short or very long responses
        if len(content) < 5:
            base_score -= 0.3
        elif len(content) > 1000:
            base_score -= 0.1
        
        # Penalize responses with error indicators
        error_indicators = ['error', 'cannot', 'unable', 'not found', 'unclear']
        if any(indicator in content.lower() for indicator in error_indicators):
            base_score -= 0.2
        
        return max(0.0, min(1.0, base_score))
    
    async def classify_industry(self, company_name: str, website_content: str, 
                              description: str = "") -> LLMResponse:
        """Classify company industry using LLM"""
        prompt = PromptTemplates.INDUSTRY_CLASSIFICATION.format(
            company_name=company_name,
            website_content=website_content[:2000],  # Limit content length
            description=description[:500]
        )
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            self.executor, 
            self._generate_response, 
            prompt, 
            "industry_classification"
        )
        
        # Log the processing
        await self._log_llm_processing(response, company_name)
        
        return response
    
    async def extract_keywords(self, company_name: str, website_content: str, 
                             about_content: str = "") -> LLMResponse:
        """Extract business keywords using LLM"""
        prompt = PromptTemplates.KEYWORD_EXTRACTION.format(
            company_name=company_name,
            website_content=website_content[:2000],
            about_content=about_content[:1000]
        )
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            self.executor, 
            self._generate_response, 
            prompt, 
            "keyword_extraction"
        )
        
        await self._log_llm_processing(response, company_name)
        return response
    
    async def estimate_company_size(self, company_name: str, website_content: str,
                                  about_content: str = "", team_content: str = "") -> LLMResponse:
        """Estimate company size using LLM"""
        prompt = PromptTemplates.COMPANY_SIZE_ESTIMATION.format(
            company_name=company_name,
            website_content=website_content[:1500],
            about_content=about_content[:1000],
            team_content=team_content[:1000]
        )
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            self.executor, 
            self._generate_response, 
            prompt, 
            "company_size_estimation"
        )
        
        await self._log_llm_processing(response, company_name)
        return response
    
    async def extract_products_services(self, company_name: str, website_content: str,
                                      products_content: str = "") -> LLMResponse:
        """Extract products and services using LLM"""
        prompt = PromptTemplates.PRODUCTS_SERVICES_EXTRACTION.format(
            company_name=company_name,
            website_content=website_content[:2000],
            products_content=products_content[:1500]
        )
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            self.executor, 
            self._generate_response, 
            prompt, 
            "products_services_extraction"
        )
        
        await self._log_llm_processing(response, company_name)
        return response
    
    async def extract_contact_info(self, company_name: str, website_content: str,
                                 contact_content: str = "") -> LLMResponse:
        """Extract contact information using LLM"""
        prompt = PromptTemplates.CONTACT_INFO_EXTRACTION.format(
            company_name=company_name,
            website_content=website_content[:1500],
            contact_content=contact_content[:1000]
        )
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            self.executor, 
            self._generate_response, 
            prompt, 
            "contact_info_extraction"
        )
        
        await self._log_llm_processing(response, company_name)
        return response
    
    async def assess_data_quality(self, company_data: Dict[str, Any]) -> LLMResponse:
        """Assess data quality using LLM"""
        prompt = PromptTemplates.DATA_QUALITY_ASSESSMENT.format(
            company_name=company_data.get('company_name', 'N/A'),
            website=company_data.get('website', 'N/A'),
            industry=company_data.get('industry', 'N/A'),
            employee_count=company_data.get('number_of_employees', 'N/A'),
            revenue=company_data.get('revenue', 'N/A'),
            contact_info=f"Email: {company_data.get('contact_email', 'N/A')}, Phone: {company_data.get('contact_phone', 'N/A')}",
            description=company_data.get('description', 'N/A')[:500]
        )
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            self.executor, 
            self._generate_response, 
            prompt, 
            "data_quality_assessment"
        )
        
        await self._log_llm_processing(response, company_data.get('company_name', 'Unknown'))
        return response
    
    async def enrich_company_batch(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich a batch of companies with LLM processing"""
        enriched_companies = []
        
        for company in companies:
            try:
                enriched_company = await self.enrich_single_company(company)
                enriched_companies.append(enriched_company)
                
                # Add small delay to avoid overwhelming the LLM
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error enriching company {company.get('company_name', 'Unknown')}: {e}")
                enriched_companies.append(company)  # Return original if enrichment fails
        
        return enriched_companies
    
    async def enrich_single_company(self, company: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single company with LLM processing"""
        enriched = company.copy()
        
        company_name = company.get('company_name', '')
        website_content = company.get('website_content', '')
        
        if not company_name or not website_content:
            logger.warning(f"Insufficient data for LLM enrichment: {company_name}")
            return enriched
        
        # Industry classification
        if not company.get('industry'):
            industry_response = await self.classify_industry(
                company_name, website_content, company.get('description', '')
            )
            if industry_response.content and not industry_response.error:
                enriched['industry'] = self._clean_industry_response(industry_response.content)
        
        # Keyword extraction
        if not company.get('keywords'):
            keywords_response = await self.extract_keywords(
                company_name, website_content, company.get('about_content', '')
            )
            if keywords_response.content and not keywords_response.error:
                enriched['keywords'] = self._parse_keywords(keywords_response.content)
        
        # Company size estimation
        if not company.get('company_size') or company.get('company_size') == 'Unknown':
            size_response = await self.estimate_company_size(
                company_name, website_content, 
                company.get('about_content', ''), company.get('team_content', '')
            )
            if size_response.content and not size_response.error:
                enriched['company_size'] = self._clean_size_response(size_response.content)
        
        # Products and services extraction
        if not company.get('products_offered') and not company.get('services_offered'):
            products_response = await self.extract_products_services(
                company_name, website_content, company.get('products_content', '')
            )
            if products_response.content and not products_response.error:
                products, services = self._parse_products_services(products_response.content)
                if products:
                    enriched['products_offered'] = products
                if services:
                    enriched['services_offered'] = services
        
        # Contact information extraction
        if not company.get('contact_email') or not company.get('contact_phone'):
            contact_response = await self.extract_contact_info(
                company_name, website_content, company.get('contact_content', '')
            )
            if contact_response.content and not contact_response.error:
                email, phone, address = self._parse_contact_info(contact_response.content)
                if email and not company.get('contact_email'):
                    enriched['contact_email'] = email
                if phone and not company.get('contact_phone'):
                    enriched['contact_phone'] = phone
        
        # Data quality assessment
        quality_response = await self.assess_data_quality(enriched)
        if quality_response.content and not quality_response.error:
            try:
                quality_score = float(re.search(r'(\d+\.?\d*)', quality_response.content).group(1))
                enriched['data_quality_score'] = min(1.0, max(0.0, quality_score))
            except (ValueError, AttributeError):
                enriched['data_quality_score'] = 0.5  # Default score
        
        return enriched
    
    def _clean_industry_response(self, response: str) -> str:
        """Clean and validate industry classification response"""
        response = response.strip()
        
        # Find the industry in the response
        for industry in settings.industries:
            if industry.lower() in response.lower():
                return industry
        
        # If no exact match, return the response as-is (might be valid)
        return response if len(response) < 50 else "Other"
    
    def _clean_size_response(self, response: str) -> str:
        """Clean and validate company size response"""
        response = response.strip()
        
        # Find the size category in the response
        for size in settings.company_sizes:
            if size.lower() in response.lower():
                return size
        
        return "Unknown"
    
    def _parse_keywords(self, response: str) -> List[str]:
        """Parse keywords from LLM response"""
        # Remove "Keywords:" prefix if present
        response = re.sub(r'^keywords?:\s*', '', response, flags=re.IGNORECASE)
        
        # Split by comma and clean
        keywords = [k.strip() for k in response.split(',')]
        keywords = [k for k in keywords if k and len(k) > 2]
        
        return keywords[:10]  # Limit to 10 keywords
    
    def _parse_products_services(self, response: str) -> Tuple[List[str], List[str]]:
        """Parse products and services from LLM response"""
        products = []
        services = []
        
        # Look for PRODUCTS: and SERVICES: sections
        products_match = re.search(r'PRODUCTS:\s*(.+?)(?=SERVICES:|$)', response, re.IGNORECASE | re.DOTALL)
        services_match = re.search(r'SERVICES:\s*(.+?)$', response, re.IGNORECASE | re.DOTALL)
        
        if products_match:
            products_text = products_match.group(1).strip()
            products = [p.strip() for p in products_text.split(';') if p.strip()]
        
        if services_match:
            services_text = services_match.group(1).strip()
            services = [s.strip() for s in services_text.split(';') if s.strip()]
        
        return products[:5], services[:5]  # Limit to 5 each
    
    def _parse_contact_info(self, response: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse contact information from LLM response"""
        email = None
        phone = None
        address = None
        
        # Extract email
        email_match = re.search(r'EMAIL:\s*([^\n]+)', response, re.IGNORECASE)
        if email_match and 'not found' not in email_match.group(1).lower():
            email = email_match.group(1).strip()
        
        # Extract phone
        phone_match = re.search(r'PHONE:\s*([^\n]+)', response, re.IGNORECASE)
        if phone_match and 'not found' not in phone_match.group(1).lower():
            phone = phone_match.group(1).strip()
        
        # Extract address
        address_match = re.search(r'ADDRESS:\s*([^\n]+)', response, re.IGNORECASE)
        if address_match and 'not found' not in address_match.group(1).lower():
            address = address_match.group(1).strip()
        
        return email, phone, address
    
    async def _log_llm_processing(self, response: LLMResponse, company_name: str):
        """Log LLM processing to database"""
        try:
            # This would typically insert into llm_processing_log table
            # For now, just log to application logs
            logger.log_llm_processing(
                response.model_name,
                response.prompt_type,
                response.tokens_used,
                response.processing_time_ms
            )
            
        except Exception as e:
            logger.error(f"Error logging LLM processing: {e}")
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current LLM model"""
        if not self.client:
            return {"error": "LLM client not initialized"}
        
        try:
            models = self.client.list()
            current_model = None
            
            for model in models.get('models', []):
                if model['name'] == self.model_name:
                    current_model = model
                    break
            
            return {
                "model_name": self.model_name,
                "model_info": current_model,
                "temperature": self.temperature,
                "max_tokens": self.max_tokens,
                "timeout": self.timeout
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def close(self):
        """Clean up resources"""
        if self.executor:
            self.executor.shutdown(wait=True)


# Global LLM enricher instance
llm_enricher = LLMEnricher()


# Convenience functions
async def enrich_company_data(company: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich single company data"""
    return await llm_enricher.enrich_single_company(company)


async def enrich_company_batch(companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enrich batch of companies"""
    return await llm_enricher.enrich_company_batch(companies)


async def classify_industry(company_name: str, content: str) -> str:
    """Classify company industry"""
    response = await llm_enricher.classify_industry(company_name, content)
    return response.content if response.content and not response.error else "Other"


async def extract_keywords(company_name: str, content: str) -> List[str]:
    """Extract keywords from company content"""
    response = await llm_enricher.extract_keywords(company_name, content)
    return llm_enricher._parse_keywords(response.content) if response.content and not response.error else []


# Example usage and testing
if __name__ == "__main__":
    async def test_llm_enricher():
        """Test LLM enricher functionality"""
        print("Testing LLM Enricher...")
        
        # Test model info
        model_info = llm_enricher.get_model_info()
        print(f"Model info: {model_info}")
        
        # Test company data
        test_company = {
            'company_name': 'TechCorp Singapore',
            'website_content': 'We are a leading technology company specializing in artificial intelligence and machine learning solutions for businesses. Our team of 50+ engineers develops cutting-edge software.',
            'about_content': 'Founded in 2018, TechCorp has grown to become a trusted partner for digital transformation.',
            'description': 'AI and ML solutions provider'
        }
        
        # Test industry classification
        industry_response = await llm_enricher.classify_industry(
            test_company['company_name'],
            test_company['website_content'],
            test_company['description']
        )
        print(f"Industry classification: {industry_response.content}")
        
        # Test keyword extraction
        keywords_response = await llm_enricher.extract_keywords(
            test_company['company_name'],
            test_company['website_content'],
            test_company['about_content']
        )
        print(f"Keywords: {keywords_response.content}")
        
        # Test full enrichment
        enriched = await llm_enricher.enrich_single_company(test_company)
        print(f"Enriched company: {enriched}")
        
        print("LLM Enricher test completed!")
    
    asyncio.run(test_llm_enricher())
