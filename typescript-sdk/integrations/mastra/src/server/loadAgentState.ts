import type { Agent as LocalMastraAgent } from "@mastra/core/agent";
import type { AGUIMessage } from "../utils/messages.js";
import { mastraMsgsToAGUI, type MastraMemoryMessage } from "../utils/messages.js";

export interface LoadAgentStateInput {
  agentId: string;
  resourceId?: string;
  threadId: string;
  limit?: number;
}

export interface AgentStateSnapshot {
  threadsExist: boolean;
  agentId: string;
  resourceId?: string;
  threadId: string;
  messages: AGUIMessage[];
  workingMemory?: Record<string, any>;
}

export async function loadAgentState(
  input: LoadAgentStateInput,
  mastraAgent: LocalMastraAgent
): Promise<AgentStateSnapshot> {
  const { agentId, resourceId, threadId, limit = 100 } = input;

  const emptySnapshot: AgentStateSnapshot = {
    threadsExist: false,
    agentId,
    resourceId,
    threadId,
    messages: [],
    workingMemory: undefined,
  };

  try {
    const memory = await mastraAgent.getMemory();
    if (!memory) {
      return emptySnapshot;
    }

    let thread;
    try {
      thread = await memory.getThreadById({ threadId });
    } catch (error) {
      return emptySnapshot;
    }

    if (!thread) {
      return emptySnapshot;
    }

    let mastraMessages: MastraMemoryMessage[] = [];
    try {
      const queryResult = await memory.query({
        threadId,
        resourceId,
      });

      mastraMessages = (queryResult.uiMessages || []) as MastraMemoryMessage[];
    } catch (error) {
      // Silently handle error
    }

    const aguiMessages = mastraMsgsToAGUI(mastraMessages);

    let workingMemory: Record<string, any> | undefined;
    if (thread.metadata?.workingMemory) {
      try {
        if (typeof thread.metadata.workingMemory === "string") {
          workingMemory = JSON.parse(thread.metadata.workingMemory);
        } else if (typeof thread.metadata.workingMemory === "object") {
          workingMemory = thread.metadata.workingMemory as Record<string, any>;
        }
      } catch (error) {
        // Silently handle error
      }
    }

    return {
      threadsExist: true,
      agentId,
      resourceId,
      threadId,
      messages: aguiMessages,
      workingMemory,
    };
  } catch (error) {
    return emptySnapshot;
  }
}
