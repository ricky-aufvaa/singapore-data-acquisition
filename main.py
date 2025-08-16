#!/usr/bin/env python3
"""
Wrapper script to run the main ETL pipeline
This allows running the pipeline with: python main.py instead of python -m src.main
"""

import sys
import os

# Add the current directory to Python path so we can import src modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import and run the main module
from src.main import main
import asyncio

if __name__ == "__main__":
    # Set up event loop policy for Windows compatibility
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # Run the main function
    asyncio.run(main())
