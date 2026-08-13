import os
from typing import Annotated, TypedDict, Literal
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_google_genai import ChatGoogleGenerativeAI
from crewai import Agent, Crew, Process, Task
from crewai_tools import SerperDevTool
from langsmith import traceable


# Initialize your preferred Gemini model wrapper via LangChain
gemini_llm = ChatGoogleGenerativeAI(
    model="gemini-3.5-flash",
    temperature=0.1
)
gemini_flash = ChatGoogleGenerativeAI(
    model="gemini/gemini-3.6-flash"
)
# Instantiate the web scraping search tool (Automatically picks up SERPER_API_KEY from environment)
web_search_tool = SerperDevTool()

# --- 1. Define the LangGraph State Schema Tracker ---
class AgentGraphState(TypedDict):
    messages: Annotated[list, add_messages]
    raw_payload: dict       
    extracted_data: str     
    validation_status: str  
    final_report: str       

# --- 2. Define CrewAI Agent Personas & Goals ---
market_researcher = Agent(
    role="Senior Wall Street Equity Analyst",
    goal="Investigate raw market data anomalies and perform deep background research on recent corporate events and corporate filings.",
    backstory="You are an elite quantitative financial researcher. You take raw, high-level numeric stock updates and use web search tools to uncover the real catalyst events behind metric shifts, volume changes, or price trends.",
    tools=[web_search_tool],
    llm='gemini/gemini-3.6-flash',
    #model=gemini_llm,
    verbose=True
)

quality_auditor = Agent(
    role="Data Quality and Compliance Officer",
    goal="Audit research data for completeness, metrics verification, and systemic depth.",
    backstory="You are a strict data auditor. You inspect research drafts. If a draft contains concrete market analysis and clear supporting arguments, you approve it. Otherwise, you flag it for revision.",
    llm='gemini/gemini-3.6-flash',
    verbose=True
)

executive_writer = Agent(
    role="Chief Communications Editor",
    goal="Compile verified financial raw briefs into pristine presentation-grade corporate executive markdown dossiers.",
    backstory="You are a world-class financial technical writer. You convert unstructured research findings into beautifully polished markdown layout reports fit for C-suite executive presentation.",
    llm='gemini/gemini-3.6-flash',
    verbose=True
)

# --- 3. Upgrading LangGraph Nodes with CrewAI Workforces ---

@traceable(name="Researcher_Node")
def researcher_node(state: AgentGraphState) -> dict:
    print("\n⚡ [Orchestration Graph] Spawning CrewAI Researcher Node...")
    raw_metrics = state["raw_payload"].get("raw_text", "No raw metric data supplied.")
    timestamp = state["raw_payload"].get("timestamp", "Current Period")
    
    # Define a focused execution task for the CrewAI researcher agent
    research_task = Task(
        description=(
            f"Review this raw market feed update for the period ending {timestamp}:\n"
            f"'{raw_metrics}'\n\n"
            f"Execute web search queries to investigate what significant corporate announcements, earnings reports, "
            f"or broader market trends occurred during or around this timeframe that directly impacted this ticker. "
            f"Compile a deep background context document connecting the numbers to the actual news events."
        ),
        expected_output="A detailed, comprehensive multi-paragraph market intelligence research brief containing explicit news correlations and context.",
        agent=market_researcher
    )
    
    # Standardize agent processing inside a localized sequential execution loop crew
    crew = Crew(agents=[market_researcher], tasks=[research_task], process=Process.sequential,verbose=True)
    result = crew.kickoff()
    
    return {
        "messages": [("assistant", "CrewAI Researcher completed analysis phase.")],
        "extracted_data": str(result)
    }

@traceable(name="Validator_Node")
def validator_node(state: AgentGraphState) -> dict:
    print("\n⚡ [Orchestration Graph] Spawning CrewAI Quality Validator Node...")
    compiled_analysis = state["extracted_data"]
    
    validation_task = Task(
        description=(
            f"Examine and audit this market analysis report draft:\n\n"
            f"'{compiled_analysis}'\n\n"
            f"Perform an exhaustive quality check. If this draft contains deep contextual news explanations and clear details, "
            f"respond with exactly the word 'Approved'. If it lacks depth or looks brief, respond with exactly the word 'RevisionNeeded'. "
            f"Do not output any other punctuation, explanations, or commentary text."
        ),
        expected_output="Exactly either 'Approved' or 'RevisionNeeded'",
        agent=quality_auditor
    )
    
    crew = Crew(agents=[quality_auditor], tasks=[validation_task], process=Process.sequential,verbose = True)
    verdict = str(crew.kickoff()).strip()
    
    # Fallback formatting normalization
    status_flag = "Approved" if "Approved" in verdict else "RevisionNeeded"
    print(f"🔍 [Quality Audit Check] CrewAI Evaluated Status -> {status_flag}")
    
    return {
        "messages": [("assistant", f"Validator status set to tracking flag: {status_flag}")],
        "validation_status": status_flag
    }

@traceable(name="Writer_Node")
def writer_node(state: AgentGraphState) -> dict:
    print("\n⚡ [Orchestration Graph] Spawning CrewAI Executive Writer Node...")
    verified_data = state["extracted_data"]
    
    write_task = Task(
        description=(
            f"Take this verified, audited market intelligence research brief:\n\n"
            f"'{verified_data}'\n\n"
            f"Synthesize the text into a clean, presentation-grade corporate executive markdown summary. "
            f"Structure the document beautifully using professional headers like '### Executive Market Summary', "
            f"'#### Real-World Catalysts & Context', and '#### Strategic Industry Impact'. Bold key metrics and entities."
        ),
        expected_output="A beautiful, production-ready corporate executive report formatted strictly in clean Markdown syntax.",
        agent=executive_writer
    )
    
    crew = Crew(agents=[executive_writer], tasks=[write_task], process=Process.sequential,verbose= True)
    formatted_markdown = crew.kickoff()
    
    return {
        "messages": [("assistant", "CrewAI Writer node final markdown rendering operations complete.")],
        "final_report": str(formatted_markdown)
    }

# --- 4. Define LangGraph Conditional Routing Logic ---
def routing_edge_logic(state: AgentGraphState) -> Literal["writer", "researcher"]:
    status = state.get("validation_status")
    if status == "Approved":
        print("🟩 [Router Directive] Quality Threshold Passed. Directing to Executive Writer Node.")
        return "writer"
    else:
        print("🟥 [Router Directive] Content Flagged Incomplete. Bouncing execution back to Researcher Node.")
        return "researcher"

# --- 5. Assemble and Compile the State Graph Configuration Workflow ---
workflow = StateGraph(AgentGraphState)

workflow.add_node("researcher", researcher_node)
workflow.add_node("validator", validator_node)
workflow.add_node("writer", writer_node)

workflow.add_edge(START, "researcher")
workflow.add_edge("researcher", "validator")

workflow.add_conditional_edges(
    "validator",
    routing_edge_logic,
    {
        "writer": "writer",       
        "researcher": "researcher" 
    }
)
workflow.add_edge("writer", END)

compiled_agent_graph = workflow.compile()
print("🎉 LangGraph + CrewAI Collaborative Engine successfully compiled with live Serper search tool integrations.")

# --- 6. Execution Testing Wrapper ---
if __name__ == "__main__":
    mock_payload = {
        "source": "Alpha Vantage Time Series (IBM)",
        "timestamp": "2026-08-07",
        "raw_text": "Weekly Market Update for IBM on week ending 2026-08-07. Metrics: Open: $190.50, High: $194.20, Low: $189.10, Close: $193.15, Volume: 3200000 shares traded."
    }
    
    print("\n--- Triggering Local Connected LangGraph + CrewAI Test Run ---")
    initial_graph_state = {"raw_payload": mock_payload, "messages": []}
    
    final_output_state = compiled_agent_graph.invoke(initial_graph_state)
    
    print("\n==========================================================================")
    print("🎉 COLLABORATIVE ENGINE RUN SUCCESSFUL! INTEGRATED MARKDOWN DOSSIER:")
    print("==========================================================================\n")
    print(final_output_state.get("final_report"))
