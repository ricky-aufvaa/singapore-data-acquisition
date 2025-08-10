"""
Rate Limiter for Singapore Company Database ETL Pipeline
Provides rate limiting functionality for web scraping and API calls
"""

import asyncio
import time
from typing import Dict, List
from collections import deque
from datetime import datetime, timedelta

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """Advanced rate limiter with multiple time window support"""
    
    def __init__(self, requests_per_second: int = 2, requests_per_minute: int = 100, requests_per_hour: int = 5000):
        self.requests_per_second = requests_per_second
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        
        # Track request timestamps
        self.request_times = deque()
        self.last_request_time = 0
        
        # Statistics
        self.total_requests = 0
        self.total_wait_time = 0
        
    async def wait(self):
        """Wait if necessary to respect all rate limits"""
        current_time = time.time()
        wait_time = 0
        
        # Per-second rate limiting
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.requests_per_second
        
        if time_since_last < min_interval:
            second_wait = min_interval - time_since_last
            wait_time = max(wait_time, second_wait)
        
        # Clean old request times
        cutoff_time = current_time - 3600  # 1 hour ago
        while self.request_times and self.request_times[0] < cutoff_time:
            self.request_times.popleft()
        
        # Per-minute rate limiting
        minute_cutoff = current_time - 60
        recent_requests_minute = sum(1 for t in self.request_times if t > minute_cutoff)
        
        if recent_requests_minute >= self.requests_per_minute:
            oldest_in_minute = min(t for t in self.request_times if t > minute_cutoff)
            minute_wait = 60 - (current_time - oldest_in_minute)
            wait_time = max(wait_time, minute_wait)
        
        # Per-hour rate limiting
        if len(self.request_times) >= self.requests_per_hour:
            hour_wait = 3600 - (current_time - self.request_times[0])
            wait_time = max(wait_time, hour_wait)
        
        # Apply wait time
        if wait_time > 0:
            logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)
            self.total_wait_time += wait_time
        
        # Record this request
        self.last_request_time = time.time()
        self.request_times.append(self.last_request_time)
        self.total_requests += 1
    
    def get_stats(self) -> Dict[str, float]:
        """Get rate limiter statistics"""
        return {
            'total_requests': self.total_requests,
            'total_wait_time': self.total_wait_time,
            'average_wait_time': self.total_wait_time / max(1, self.total_requests),
            'requests_in_last_minute': sum(1 for t in self.request_times if time.time() - t < 60),
            'requests_in_last_hour': len(self.request_times)
        }
    
    def reset_stats(self):
        """Reset statistics"""
        self.total_requests = 0
        self.total_wait_time = 0


class DomainRateLimiter:
    """Rate limiter that maintains separate limits per domain"""
    
    def __init__(self, default_requests_per_second: int = 2):
        self.default_requests_per_second = default_requests_per_second
        self.domain_limiters: Dict[str, RateLimiter] = {}
        
        # Domain-specific configurations
        self.domain_configs = {
            'linkedin.com': {'requests_per_second': 0.5, 'requests_per_minute': 20},
            'facebook.com': {'requests_per_second': 1, 'requests_per_minute': 30},
            'yellowpages.com.sg': {'requests_per_second': 2, 'requests_per_minute': 100},
            'acra.gov.sg': {'requests_per_second': 1, 'requests_per_minute': 50},
        }
    
    def _get_domain_from_url(self, url: str) -> str:
        """Extract domain from URL"""
        from urllib.parse import urlparse
        return urlparse(url).netloc.lower()
    
    def _get_limiter_for_domain(self, domain: str) -> RateLimiter:
        """Get or create rate limiter for domain"""
        if domain not in self.domain_limiters:
            config = self.domain_configs.get(domain, {})
            
            self.domain_limiters[domain] = RateLimiter(
                requests_per_second=config.get('requests_per_second', self.default_requests_per_second),
                requests_per_minute=config.get('requests_per_minute', 100),
                requests_per_hour=config.get('requests_per_hour', 5000)
            )
        
        return self.domain_limiters[domain]
    
    async def wait_for_url(self, url: str):
        """Wait for rate limit based on URL domain"""
        domain = self._get_domain_from_url(url)
        limiter = self._get_limiter_for_domain(domain)
        await limiter.wait()
    
    def get_domain_stats(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all domains"""
        return {
            domain: limiter.get_stats() 
            for domain, limiter in self.domain_limiters.items()
        }


class AdaptiveRateLimiter:
    """Rate limiter that adapts based on server responses"""
    
    def __init__(self, initial_requests_per_second: int = 2):
        self.current_requests_per_second = initial_requests_per_second
        self.base_limiter = RateLimiter(requests_per_second=initial_requests_per_second)
        
        # Adaptive parameters
        self.success_count = 0
        self.error_count = 0
        self.last_adjustment = time.time()
        self.adjustment_interval = 60  # Adjust every minute
        
        # Rate adjustment factors
        self.increase_factor = 1.2
        self.decrease_factor = 0.8
        self.min_rate = 0.1
        self.max_rate = 10
    
    async def wait(self):
        """Wait with adaptive rate limiting"""
        await self.base_limiter.wait()
        
        # Check if it's time to adjust rate
        if time.time() - self.last_adjustment > self.adjustment_interval:
            self._adjust_rate()
    
    def record_success(self):
        """Record a successful request"""
        self.success_count += 1
    
    def record_error(self, error_type: str = 'generic'):
        """Record a failed request"""
        self.error_count += 1
        
        # Immediate rate reduction for certain errors
        if error_type in ['rate_limit', 'too_many_requests', '429']:
            self._reduce_rate_immediately()
    
    def _adjust_rate(self):
        """Adjust rate based on success/error ratio"""
        total_requests = self.success_count + self.error_count
        
        if total_requests > 0:
            success_rate = self.success_count / total_requests
            
            if success_rate > 0.95:  # Very high success rate - increase speed
                new_rate = min(self.current_requests_per_second * self.increase_factor, self.max_rate)
            elif success_rate < 0.8:  # Low success rate - decrease speed
                new_rate = max(self.current_requests_per_second * self.decrease_factor, self.min_rate)
            else:
                new_rate = self.current_requests_per_second  # Keep current rate
            
            if new_rate != self.current_requests_per_second:
                logger.info(f"Adjusting rate limit: {self.current_requests_per_second:.2f} -> {new_rate:.2f} req/sec")
                self.current_requests_per_second = new_rate
                self.base_limiter = RateLimiter(requests_per_second=new_rate)
        
        # Reset counters
        self.success_count = 0
        self.error_count = 0
        self.last_adjustment = time.time()
    
    def _reduce_rate_immediately(self):
        """Immediately reduce rate due to rate limiting error"""
        new_rate = max(self.current_requests_per_second * 0.5, self.min_rate)
        logger.warning(f"Rate limit hit - reducing rate: {self.current_requests_per_second:.2f} -> {new_rate:.2f} req/sec")
        
        self.current_requests_per_second = new_rate
        self.base_limiter = RateLimiter(requests_per_second=new_rate)


# Global rate limiter instances
default_rate_limiter = RateLimiter()
domain_rate_limiter = DomainRateLimiter()
adaptive_rate_limiter = AdaptiveRateLimiter()


# Convenience functions
async def wait_for_rate_limit():
    """Wait for default rate limit"""
    await default_rate_limiter.wait()


async def wait_for_url(url: str):
    """Wait for rate limit based on URL domain"""
    await domain_rate_limiter.wait_for_url(url)


def record_request_success():
    """Record successful request for adaptive limiting"""
    adaptive_rate_limiter.record_success()


def record_request_error(error_type: str = 'generic'):
    """Record failed request for adaptive limiting"""
    adaptive_rate_limiter.record_error(error_type)


# Example usage and testing
if __name__ == "__main__":
    async def test_rate_limiter():
        """Test rate limiter functionality"""
        print("Testing rate limiter...")
        
        # Test basic rate limiter
        limiter = RateLimiter(requests_per_second=5)
        
        start_time = time.time()
        for i in range(10):
            await limiter.wait()
            print(f"Request {i+1} at {time.time() - start_time:.2f}s")
        
        print(f"Stats: {limiter.get_stats()}")
        
        # Test domain rate limiter
        print("\nTesting domain rate limiter...")
        domain_limiter = DomainRateLimiter()
        
        urls = [
            'https://www.linkedin.com/company/test',
            'https://www.facebook.com/test',
            'https://www.yellowpages.com.sg/test'
        ]
        
        for url in urls:
            await domain_limiter.wait_for_url(url)
            print(f"Processed {url}")
        
        print(f"Domain stats: {domain_limiter.get_domain_stats()}")
    
    asyncio.run(test_rate_limiter())
