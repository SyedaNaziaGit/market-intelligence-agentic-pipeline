import asyncio
import os
import sys
import uvicorn
from dotenv import load_dotenv

# Import our active streaming blocks from Day 2
from data_producer import run_data_producer_loop
from ingestion_service.app import app as fastapi_app

# --- Explicit Windows Virtual Environment Path Injection ---
# This forces sub-processes and thread executors to look inside your active myenv folder
VENV_PACKAGES = os.path.abspath(os.path.join(os.path.dirname(__file__), "myenv", "Lib", "site-packages"))
if os.path.exists(VENV_PACKAGES) and VENV_PACKAGES not in sys.path:
    sys.path.insert(0, VENV_PACKAGES)

# Ensure Python can also find your core Django project root
DJANGO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "my_django_project"))
if DJANGO_ROOT not in sys.path:
    sys.path.insert(0, DJANGO_ROOT)

load_dotenv()

async def launch_services_concurrently():
    print("\n========================================================")
    print("🔥 BOOTING ALL UNIFIED INGESTION SYSTEM INSTANCES 🔥")
    print("========================================================\n")

    # 1. Start the Data Producer script as an active concurrent background worker task
    producer_worker = asyncio.create_task(run_data_producer_loop())
    
    # 2. Spin up Uvicorn Web Server to wrap around the FastAPI Consumer Hub lifespan app
    config = uvicorn.Config(
        app=fastapi_app, 
        host="127.0.0.1", 
        port=8000, 
        log_level="info"
    )
    server = uvicorn.Server(config)
    
    try:
        # Run Uvicorn server loops natively within the concurrent task context
        await server.serve()
    except KeyboardInterrupt:
        print("\nShutdown signal caught.")
    finally:
        print("Stopping upstream background producer threads...")
        producer_worker.cancel()
        await asyncio.gather(producer_worker, return_exceptions=True)
        print("System safely offline.")

if __name__ == "__main__":
    asyncio.run(launch_services_concurrently())
