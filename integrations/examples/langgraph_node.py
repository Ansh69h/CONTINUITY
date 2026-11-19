"""Minimal usage; provide the application's own LangGraph checkpointer."""

from typing import TypedDict

from continuity import ContinuityRuntime, SQLiteCausalStore, SideEffectClass
from continuity_integrations.langgraph import durable_node
from langgraph.graph import END, START, StateGraph


class State(TypedDict):
    value: int
    doubled: int


store = SQLiteCausalStore("workflow.db")


def runtime_for_thread(thread_id: str) -> ContinuityRuntime:
    return ContinuityRuntime(store=store, run_id=f"graph:{thread_id}")


@durable_node(runtime_for_thread, step_id="double", side_effect=SideEffectClass.PURE)
def double(state: State, config):
    return {"doubled": state["value"] * 2}


graph = StateGraph(State)
graph.add_node("double", double)
graph.add_edge(START, "double")
graph.add_edge("double", END)
app = graph.compile()  # Supply your own LangGraph checkpointer when graph-state persistence is needed.
result = app.invoke({"value": 21, "doubled": 0}, config={"configurable": {"thread_id": "demo"}})
print(result)
