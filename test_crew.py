from crewai import Agent, Crew, Task, LLM

# Configure Gemini 3.6 Flash cleanly
gemini_flash = LLM(
    model="gemini/gemini-3.6-flash"
)

# Assign it to your agent
researcher = Agent(
    role='Data Specialist',
    goal='Synthesize rapid data summaries',
    backstory='An expert automated agent optimized for speed.',
    llm=gemini_flash,
    verbose=True
)

# Example Task
task = Task(
    description='List 3 benefits of using lightweight LLM orchestration.',
    expected_output='A quick 3-bullet summary.',
    agent=researcher
)

crew = Crew(
    agents=[researcher],
    tasks=[task],
    verbose=True
)

crew.kickoff()
