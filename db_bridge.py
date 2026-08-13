import os
import sys
import django
from datetime import datetime
import logging

# --- Setup Telemetry Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DjangoORMBreaker")

# --- 1. Bootstrapping Independent Django ORM Environment ---
# Point Python directly to your Django project root module folder
DJANGO_PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "my_django_project"))
if DJANGO_PROJECT_PATH not in sys.path:
    sys.path.append(DJANGO_PROJECT_PATH)

# Set the environment variable pointer to your settings file module context
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_django_project.settings")

# Trigger initial setups to register models safely inside standalone thread pools
django.setup()

# Import your custom database report model natively from the application layer
from intelligence_app.models import MarketIntelligenceReport

def save_agent_output_to_database(graph_final_state: dict) -> bool:
    """
    Parses the terminal state dictionary out of the finished LangGraph execution loop,
    calculates telemetry metadata metrics, and commits a clean record row via Django ORM.
    """
    logger.info("💾 [Database Layer] Initiating Django ORM persistence bridge loop...")
    
    try:
        raw_payload = graph_final_state.get("raw_payload", {})
        
        # Parse timestamp formats safely from the stream inputs
        raw_time = raw_payload.get("timestamp")
        if isinstance(raw_time, list) and len(raw_time) > 0:
            raw_time = raw_time[0]
            
        try:
            # Handles Alpha Vantage standard format '2026-08-07'
            parsed_timestamp = datetime.strptime(raw_time, "%Y-%m-%d")
        except:
            parsed_timestamp = datetime.now()

        # Calculate how many loop bounces occurred by evaluating state message traces
        messages = graph_final_state.get("messages", [])
        loop_counter = sum(1 for msg in messages if "Validator status set to" in getattr(msg, 'content', str(msg)))
        
        # Instantiate the database table row mapping object instance
        db_record = MarketIntelligenceReport(
            source_feed=raw_payload.get("source", "API Data Feed Stream"),
            original_alert_text=raw_payload.get("raw_text", "Empty Event Source Content Data"),
            source_timestamp=parsed_timestamp,
            validation_loops_count=max(1, loop_counter),
            status=graph_final_state.get("validation_status", "Approved"),
            generated_markdown_report=graph_final_state.get("final_report", "# Ingestion Rendering Failure Exception")
        )
        
        # Commit the transaction row directly to local sqlite storage
        db_record.save()
        logger.info(f"✨ SUCCESS! Saved Market Intelligence Report Table Row ID: [{db_record.id}] directly via Django ORM.")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to persist agentic state results to the database: {str(e)}")
        return False
