import asyncio
import os
import json
import logging
import requests
from dotenv import load_dotenv
from stream_broker import EmbeddedQueueBroker

# Load local environment blocks
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MarketIntelligenceProducer")

ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
TOPIC_NAME = "market-intelligence-stream"
TARGET = "IBM"
# Alpha Vantage real-time financial market news & sentiment feed URL
#API_URL = f"https://alphavantage.co{ALPHA_VANTAGE_KEY}"
API_URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={TARGET}&interval=5min&apikey={ALPHA_VANTAGE_KEY}"
async def run_data_producer_loop():
    logger.info("Initializing Live Market Intelligence Polling Feed...")
    
    while True:
        try:
            # Poll data synchronously over an executor thread block to prevent event loop stalls
            logger.info("Polling upstream news tickers from Alpha Vantage API...")
            response = await asyncio.to_thread(requests.get, API_URL, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                articles = data.get("feed", [])
                
                if not articles:
                    logger.warning("API call succeeded but returned an empty feed array. Checking rate limits...")
                    # Fallback structural mock data to keep development active if API limits hit
                    articles = [{
                        "source": "Mock Financial Engine",
                        "title": "Gemini models see massive enterprise adoption surge.",
                        "summary": "AI systems engineering shifts quickly from simple wrappers to multi-agent architectures using LangGraph.",
                        "time_published": "20260811T120000"
                    }]

                for article in articles[:5]:  # Process the top 5 freshest insights per batch
                    payload = {
                        "source": article.get("source", "Global Financial Feed"),
                        "raw_text": f"{article.get('title', '')} - {article.get('summary', '')}",
                        "timestamp": article.get("time_published", "")
                    }
                    
                    # Convert to byte stream packets mimicking standard Kafka execution blocks
                    serialized_data = json.dumps(payload).encode('utf-8')
                    
                    # Publish event to the queue
                    await EmbeddedQueueBroker.produce(TOPIC_NAME, serialized_data)
                    await asyncio.sleep(1) # Slight staging delay between packet dumps
                    
            else:
                logger.error(f"Upstream API returned error code status: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Ingestion extraction failure instance: {str(e)}")
            
        # Rate limit spacing layer: wait 60 seconds before executing next batch
        logger.info("Batch streaming cycle complete. Sleeping for 60 seconds...")
        await asyncio.sleep(60)

if __name__ == "__main__":
    # For standalone execution unit validation testing
    asyncio.run(run_data_producer_loop())
