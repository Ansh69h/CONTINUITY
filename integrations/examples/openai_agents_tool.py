"""Local function tool with a durable handler; requires an Agents SDK setup."""

from agents import Agent, Runner
import asyncio

from continuity import ContinuityRuntime, SQLiteCausalStore, SideEffectClass
from continuity_integrations.openai_agents import durable_function_tool


runtime = ContinuityRuntime(store=SQLiteCausalStore("agent.db"), run_id="assistant-demo")


@durable_function_tool(runtime, step_id="lookup-account", side_effect=SideEffectClass.IDEMPOTENT)
def lookup_account(account_id: str) -> str:
    """Return a mock account label."""
    return f"account:{account_id}"


agent = Agent(name="Account helper", instructions="Use the account lookup tool.", tools=[lookup_account])


async def main():
    result = await Runner.run(agent, "Look up account A-42")
    print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())
