import asyncio
from data_producer import run_data_producer_loop
from stream_broker import EmbeddedQueueBroker

async def main():
    # 1. Start the data producer as a background task thread loop
    producer_task = asyncio.create_task(run_data_producer_loop())
    
    print("\n--- Pipeline Verification Auditing Active ---")
    print("Waiting 5 seconds for data packet collection...\n")
    await asyncio.sleep(5)
    
    # 2. Try polling an event from the broker to check serialization
    event = await EmbeddedQueueBroker.poll(timeout=2.0)
    if event:
        topic, data_bytes = event
        print("✅ SUCCESS! Event captured successfully from broker stream.")
        print(f"Topic Target Name: {topic}")
        print(f"Deserialized Ingest Data: {data_bytes.decode('utf-8')}\n")
    else:
        print("❌ FAILURE: No events detected inside stream queue buffers.")
        
    # Cancel background looping task cleanly
    producer_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
