import os
import sys

# --- FORCE HIGH-PRIORITY PATH OVERRIDES FIRST ---
# 1. Inject virtual environment paths explicitly to prevent ModuleNotFound errors
VENV_PACKAGES = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "myenv", "Lib", "site-packages"))
if os.path.exists(VENV_PACKAGES) and VENV_PACKAGES not in sys.path:
    sys.path.insert(0, VENV_PACKAGES)

# 2. Inject Django root folder path
DJANGO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "my_django_project"))
if DJANGO_ROOT not in sys.path:
    sys.path.insert(0, DJANGO_ROOT)

# 3. Initialize Django before importing models or routing services
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_django_project.settings")
import django
try:
    django.setup()
except Exception as e:
    print(f"Django setup structural warning: {str(e)}")

# --- NOW SAFELY IMPORT REMAINING FRAMEWORKS ---
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel

# Import stream and data elements safely
from stream_broker import EmbeddedQueueBroker


# --- Setup Logging Architecture ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FastAPIConsumerHub")

# --- Ingestion Schema Validation Rules ---
class HeartbeatStatus(BaseModel):
    status: str
    queue_monitoring: bool

class EventPayloadSchema(BaseModel):
    source: str
    timestamp: str
    raw_text: str

# --- Asynchronous Background Consumer Loop ---
class PipelineConsumerEngine:
    def __init__(self):
        self.active = False
        self.worker_task = None

    async def initialize_polling_loop(self):
        """Persistent background worker consumer loop."""
        self.active = True
        logger.info("🚀 Background Event Consumer Engine successfully triggered.")
        while self.active:
            try:
                # Poll the embedded stream broker queue asynchronously with a 1.0 second timeout
                event_packet = await EmbeddedQueueBroker.poll(timeout=1.0)
                
                if event_packet is None:
                    # Queue buffer empty, continue polling cycle smoothly
                    continue
                    
                topic, data_bytes = event_packet
                logger.info(f"📥 [Consumer Hub] Event Packet intercepted from topic: '{topic}'")
                
                
                # Deserialize byte text back into structured python dictionary keys
                payload_dict = json.loads(data_bytes.decode('utf-8'))
                
                # Validate payload map fields
                logger.info(f"📊 Extracted Market Core Meta: Source: {payload_dict.get('source')}")
                
                # --- Step-Off Processing Hand Off (Offloaded Thread execution hook) ---
                # On Day 4 and Day 6, we will plug the LangGraph pipeline execution here.
                # We use asyncio.to_thread to execute the heavy AI generation blocks inside 
                # a dedicated pool, keeping the core FastAPI event loop perfectly responsive.
                await asyncio.to_thread(self._mock_downstream_agent_handoff, payload_dict)
                
                # Acknowledge memory completion task loop
                EmbeddedQueueBroker.task_done()
                
            except Exception as e:
                logger.error(f"❌ Exception caught inside consumer execution loop: {str(e)}")
                await asyncio.sleep(2) # Prevent rapid fire log errors if connections snap

    def _mock_downstream_agent_handoff(self, payload: dict):
        """Passes real-time data payloads straight to LangGraph and saves results via Django ORM."""
        logger.info("🧠 Initializing real-time LangGraph Multi-Agent Orchestration workflow state...")
        
        from ingestion_service.agent_orchestrator import compiled_agent_graph
        from db_bridge import save_agent_output_to_database
        
        # Initialize the starting system state dictionary mapping
        initial_execution_state = {
            "raw_payload": payload,
            "messages": []
        }
        
        # Execute the multi-agent orchestration tree loop synchronously inside the thread pool
        final_graph_state = compiled_agent_graph.invoke(initial_execution_state)
        logger.info("🤖 Multi-Agent execution loop completed processing.")
        
        # Pipe the final state dictionary directly into your local database using Django ORM
        save_agent_output_to_database(final_graph_state)
    '''
    def _mock_downstream_agent_handoff(self, payload: dict):
        """Simulates heavy synchronous processing blocks safely inside a thread executor."""
        logger.info("🧠 Passing telemetry payload downstream to LangGraph Agent orchestrator nodes...")
        # Simulated small latency delay for local worker tracking verification
        import time
        time.sleep(0.5)
        logger.info("✅ Downstream node processing simulated successfully.")
    '''
    def stop_engine(self):
        """Triggers flag termination during app shutdown sequences."""
        logger.info("🛑 Signalling Background Consumer Engine to terminate processing loops...")
        self.active = False

# --- App Lifecycle State Orchestrator Management ---
@asynccontextmanager
async def lifecycle_manager(app: FastAPI):
    # Execution Block: Triggered exactly when Uvicorn boots up the microservice
    consumer_engine = PipelineConsumerEngine()
    loop = asyncio.get_running_loop()
    
    # Spawn the polling runner task directly into the active event execution loop
    app.state.worker_task = loop.create_task(consumer_engine.initialize_polling_loop())
    yield
    # Cleanup Block: Triggered exactly when the server signals termination (Ctrl + C)
    consumer_engine.stop_engine()
    await app.state.worker_task
    logger.info("🔌 Core Ingestion App shutdown lifecycle successfully complete.")

# --- Instantiating the FastAPI Web Application ---
app = FastAPI(
    title="AI Pipeline Event Ingestion Core Service",
    version="1.0.0",
    lifespan=lifecycle_manager
)

@app.get("/health", response_model=HeartbeatStatus)
async def check_health_status():
    """Health check node interface endpoint for system health verification."""
    return {
        "status": "healthy",
        "queue_monitoring": True
    }
