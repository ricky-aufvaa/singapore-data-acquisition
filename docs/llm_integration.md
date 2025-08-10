# LLM Integration Guide - Singapore Company Database

## Overview

This document details the integration of Llama 3 8B model via Ollama for intelligent data enrichment in the Singapore Company Database ETL pipeline. The LLM is used to extract insights, classify industries, and enhance data quality through natural language processing.

## Architecture

```
Company Data → LLM Enricher → Prompt Templates → Ollama/Llama 3 → Processed Response → Database
```

## Model Selection

### Chosen Model: Llama 3 8B

**Rationale:**
- **Open Source**: No licensing costs or API restrictions
- **Performance**: Excellent balance of accuracy and speed
- **Size**: 8B parameters provide good performance while being resource-efficient
- **Local Deployment**: Full control over data privacy and processing
- **Community Support**: Active development and optimization

**Alternative Models Considered:**
- **Mistral 7B**: Slightly smaller, good performance
- **Zephyr 7B**: Instruction-tuned, good for specific tasks
- **Phi-3 Mini**: Very efficient but limited capability
- **Code Llama**: Specialized for code, not suitable for business data

## Installation & Setup

### 1. Install Ollama

```bash
# Linux/macOS
curl -fsSL https://ollama.ai/install.sh | sh

# Windows
# Download from https://ollama.ai/download/windows
```

### 2. Pull Llama 3 Model

```bash
ollama pull llama3:8b
```

### 3. Verify Installation

```bash
ollama list
ollama run llama3:8b "Hello, test message"
```

### 4. Configure Environment

```bash
# .env file
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=llama3:8b
LLM_TEMPERATURE=0.1
LLM_MAX_TOKENS=500
```

## Use Cases & Prompt Templates

### 1. Industry Classification

**Purpose**: Classify companies into predefined industry categories

**Prompt Template**:
```
Analyze the following company information and classify it into ONE of these industries:
Technology, FinTech, Healthcare, E-commerce, Manufacturing, Professional Services, Real Estate, F&B, Education, Logistics, Construction, Retail, Energy, Media, Automotive, Agriculture, Tourism, Government, Non-Profit, Other

Company Name: {company_name}
Website Content: {website_content}
Description: {description}

Consider the company's primary business activity, products, and services. Return ONLY the industry name from the list above.

Industry:
```

**Example Input**:
```
Company Name: TechCorp Singapore
Website Content: We are a leading technology company specializing in artificial intelligence and machine learning solutions for businesses. Our team of 50+ engineers develops cutting-edge software.
Description: AI and ML solutions provider
```

**Expected Output**: `Technology`

### 2. Keyword Extraction

**Purpose**: Extract relevant business keywords from company content

**Prompt Template**:
```
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

Keywords:
```

**Example Output**: `artificial intelligence, machine learning, software development, business solutions, data analytics, automation, consulting`

### 3. Company Size Estimation

**Purpose**: Estimate company size based on textual cues

**Prompt Template**:
```
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

Company Size:
```

### 4. Products & Services Extraction

**Purpose**: Extract structured product and service information

**Prompt Template**:
```
Extract the main products and services offered by this company from the provided information.

Company Name: {company_name}
Website Content: {website_content}
Products/Services Pages: {products_content}

Separate products and services clearly. Be specific and avoid marketing language.

Format your response as:
PRODUCTS: [list products separated by semicolons]
SERVICES: [list services separated by semicolons]

If no clear distinction, list everything under SERVICES.

Response:
```

### 5. Contact Information Extraction

**Purpose**: Extract structured contact information

**Prompt Template**:
```
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

Response:
```

### 6. Data Quality Assessment

**Purpose**: Assess overall data quality and completeness

**Prompt Template**:
```
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

Quality Score:
```

## Implementation Details

### LLM Enricher Class

```python
class LLMEnricher:
    def __init__(self):
        self.client = ollama
        self.model_name = "llama3:8b"
        self.temperature = 0.1
        self.max_tokens = 500
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    async def enrich_single_company(self, company: Dict[str, Any]) -> Dict[str, Any]:
        # Industry classification
        if not company.get('industry'):
            industry_response = await self.classify_industry(...)
            company['industry'] = self._clean_industry_response(industry_response.content)
        
        # Keyword extraction
        if not company.get('keywords'):
            keywords_response = await self.extract_keywords(...)
            company['keywords'] = self._parse_keywords(keywords_response.content)
        
        # Additional enrichments...
        return company
```

### Response Processing

```python
def _parse_keywords(self, response: str) -> List[str]:
    """Parse keywords from LLM response"""
    response = re.sub(r'^keywords?:\s*', '', response, flags=re.IGNORECASE)
    keywords = [k.strip() for k in response.split(',')]
    keywords = [k for k in keywords if k and len(k) > 2]
    return keywords[:10]  # Limit to 10 keywords

def _calculate_confidence_score(self, content: str, prompt_type: str) -> float:
    """Calculate confidence score based on response characteristics"""
    base_score = 0.7
    
    if prompt_type == "industry_classification":
        if any(industry.lower() in content.lower() for industry in INDUSTRIES):
            base_score += 0.2
    
    # Penalize very short or error responses
    if len(content) < 5:
        base_score -= 0.3
    
    return max(0.0, min(1.0, base_score))
```

## Performance Optimization

### 1. Batch Processing

```python
async def enrich_company_batch(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process companies in batches for efficiency"""
    enriched_companies = []
    
    for company in companies:
        enriched_company = await self.enrich_single_company(company)
        enriched_companies.append(enriched_company)
        
        # Small delay to avoid overwhelming the LLM
        await asyncio.sleep(0.1)
    
    return enriched_companies
```

### 2. Concurrent Processing

```python
# Use ThreadPoolExecutor for CPU-bound LLM operations
loop = asyncio.get_event_loop()
response = await loop.run_in_executor(
    self.executor, 
    self._generate_response, 
    prompt, 
    "industry_classification"
)
```

### 3. Caching Strategy

```python
# Cache LLM responses to avoid reprocessing
@lru_cache(maxsize=1000)
def _cached_llm_call(self, prompt_hash: str, prompt_type: str):
    return self._generate_response(prompt, prompt_type)
```

## Quality Assurance

### 1. Response Validation

```python
def _validate_industry_response(self, response: str) -> bool:
    """Validate industry classification response"""
    valid_industries = [
        'Technology', 'FinTech', 'Healthcare', 'E-commerce',
        'Manufacturing', 'Professional Services', 'Real Estate',
        'F&B', 'Education', 'Logistics', 'Construction', 'Retail',
        'Energy', 'Media', 'Automotive', 'Agriculture', 'Tourism',
        'Government', 'Non-Profit', 'Other'
    ]
    
    return any(industry.lower() in response.lower() for industry in valid_industries)
```

### 2. Confidence Scoring

Each LLM response includes a confidence score based on:
- Response format compliance
- Content length appropriateness
- Presence of expected patterns
- Absence of error indicators

### 3. Fallback Mechanisms

```python
def _get_fallback_industry(self, company_name: str, website_content: str) -> str:
    """Fallback industry classification using keyword matching"""
    tech_keywords = ['software', 'technology', 'digital', 'IT', 'tech']
    finance_keywords = ['bank', 'finance', 'investment', 'trading']
    
    content_lower = (company_name + ' ' + website_content).lower()
    
    if any(keyword in content_lower for keyword in tech_keywords):
        return 'Technology'
    elif any(keyword in content_lower for keyword in finance_keywords):
        return 'FinTech'
    
    return 'Other'
```

## Monitoring & Logging

### 1. Performance Metrics

```python
async def _log_llm_processing(self, response: LLMResponse, company_name: str):
    """Log LLM processing metrics"""
    logger.log_llm_processing(
        response.model_name,
        response.prompt_type,
        response.tokens_used,
        response.processing_time_ms
    )
    
    # Store in database for analysis
    await self._store_llm_metrics(response, company_name)
```

### 2. Quality Tracking

Track LLM performance metrics:
- **Processing Time**: Average response time per prompt type
- **Token Usage**: Tokens consumed per operation
- **Confidence Scores**: Distribution of confidence levels
- **Error Rates**: Failed or invalid responses
- **Throughput**: Companies processed per hour

### 3. Database Logging

```sql
-- LLM processing log table
CREATE TABLE llm_processing_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    uen VARCHAR(20) REFERENCES companies(uen),
    model_name VARCHAR(100) NOT NULL,
    prompt_type VARCHAR(50) NOT NULL,
    input_text TEXT,
    output_text TEXT,
    confidence_score DECIMAL(3,2),
    processing_time_ms INTEGER,
    tokens_used INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Error Handling

### 1. Connection Failures

```python
def _handle_ollama_connection_error(self, error: Exception) -> LLMResponse:
    """Handle Ollama connection failures"""
    logger.error(f"Ollama connection failed: {error}")
    
    return LLMResponse(
        content="",
        confidence_score=0.0,
        processing_time_ms=0,
        tokens_used=0,
        model_name=self.model_name,
        prompt_type="error",
        error=str(error)
    )
```

### 2. Timeout Handling

```python
async def _generate_with_timeout(self, prompt: str, timeout: int = 30) -> str:
    """Generate response with timeout"""
    try:
        return await asyncio.wait_for(
            self._generate_response(prompt),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        logger.warning(f"LLM request timed out after {timeout}s")
        return ""
```

### 3. Graceful Degradation

```python
async def enrich_single_company(self, company: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich company with graceful degradation"""
    enriched = company.copy()
    
    try:
        # Attempt LLM enrichment
        enriched = await self._llm_enrich(company)
    except Exception as e:
        logger.warning(f"LLM enrichment failed for {company.get('company_name')}: {e}")
        # Fall back to rule-based enrichment
        enriched = self._rule_based_enrich(company)
    
    return enriched
```

## Best Practices

### 1. Prompt Engineering

- **Be Specific**: Clear, unambiguous instructions
- **Provide Examples**: Show expected output format
- **Limit Scope**: Constrain responses to valid options
- **Use Structure**: Consistent formatting for parsing

### 2. Resource Management

- **Connection Pooling**: Reuse Ollama connections
- **Memory Management**: Clear large prompts after processing
- **CPU Utilization**: Balance concurrent requests
- **Disk Space**: Monitor model storage requirements

### 3. Data Privacy

- **Local Processing**: All data stays within infrastructure
- **No External APIs**: No data sent to third-party services
- **Audit Trails**: Log all processing activities
- **Access Control**: Restrict model access appropriately

## Troubleshooting

### Common Issues

**1. Model Not Found**
```bash
# Solution: Pull the model
ollama pull llama3:8b
```

**2. Connection Refused**
```bash
# Solution: Start Ollama service
ollama serve
```

**3. Out of Memory**
```bash
# Solution: Use smaller model or increase RAM
ollama pull llama3:7b  # Smaller alternative
```

**4. Slow Performance**
```bash
# Solution: Enable GPU acceleration
# Ensure NVIDIA drivers and CUDA are installed
nvidia-smi  # Check GPU availability
```

### Performance Tuning

**1. Model Parameters**
```python
# Optimize for speed vs quality
options = {
    'temperature': 0.1,      # Lower = more deterministic
    'top_p': 0.9,           # Nucleus sampling
    'top_k': 40,            # Top-k sampling
    'num_predict': 500,     # Max tokens
    'repeat_penalty': 1.1   # Avoid repetition
}
```

**2. Hardware Optimization**
- **CPU**: 8+ cores recommended
- **RAM**: 16GB+ for 8B model
- **GPU**: NVIDIA GPU with 8GB+ VRAM (optional)
- **Storage**: SSD for model files

## Integration Testing

### Unit Tests

```python
async def test_industry_classification():
    """Test industry classification functionality"""
    enricher = LLMEnricher()
    
    test_company = {
        'company_name': 'TechCorp Singapore',
        'website_content': 'AI and machine learning solutions',
        'description': 'Technology company'
    }
    
    response = await enricher.classify_industry(
        test_company['company_name'],
        test_company['website_content'],
        test_company['description']
    )
    
    assert response.content in ['Technology', 'FinTech']
    assert response.confidence_score > 0.5
```

### Integration Tests

```python
async def test_full_enrichment_pipeline():
    """Test complete enrichment pipeline"""
    companies = [
        {'company_name': 'Test Corp', 'website_content': 'Software development'},
        {'company_name': 'Food Co', 'website_content': 'Restaurant chain'}
    ]
    
    enriched = await llm_enricher.enrich_company_batch(companies)
    
    assert len(enriched) == 2
    assert all('industry' in company for company in enriched)
    assert all('keywords' in company for company in enriched)
```

## Conclusion

The LLM integration provides significant value in enriching Singapore company data through:

1. **Intelligent Classification**: Accurate industry categorization
2. **Keyword Extraction**: Relevant business terms and concepts
3. **Data Quality Assessment**: Automated completeness scoring
4. **Structured Extraction**: Contact information and business details

The Llama 3 8B model via Ollama offers an optimal balance of performance, cost, and privacy for this use case, enabling local processing of sensitive business data while maintaining high accuracy and throughput.

Key success factors:
- **Robust Error Handling**: Graceful degradation and fallbacks
- **Performance Optimization**: Efficient batch processing and caching
- **Quality Assurance**: Validation and confidence scoring
- **Comprehensive Monitoring**: Detailed logging and metrics

This integration significantly enhances the value and usability of the Singapore company database by providing AI-powered insights and data enrichment capabilities.
