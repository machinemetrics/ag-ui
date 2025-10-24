import type { Agent as LocalMastraAgent } from "@mastra/core/agent";
import type { AGUIMessage } from "../utils/messages.js";
import { mastraMsgsToAGUI, type MastraMemoryMessage } from "../utils/messages.js";

/**
 * Input parameters for loading agent state from thread history
 */
export interface LoadAgentStateInput {
  /** The unique identifier for the agent */
  agentId: string;
  /**
   * Resource identifier used to scope threads and messages to a specific user, tenant, or group.
   * When provided, only messages associated with this resourceId will be loaded.
   * When omitted, the query may return messages without resource scoping (use with caution in multi-tenant environments).
   */
  resourceId?: string;
  /** The unique identifier for the conversation thread */
  threadId: string;
  /** Maximum number of messages to load (default: 100) */
  limit?: number;
}

/**
 * Snapshot of agent state loaded from thread history
 */
export interface AgentStateSnapshot {
  /** Whether any thread data exists for this thread */
  threadsExist: boolean;
  /** The agent identifier */
  agentId: string;
  /** The resource identifier used for scoping (may be undefined if not provided) */
  resourceId?: string;
  /** The thread identifier */
  threadId: string;
  /** Historical messages from the thread */
  messages: AGUIMessage[];
  /** Optional working memory state from the thread metadata */
  workingMemory?: Record<string, any>;
}

/**
 * Loads historical agent state (messages and working memory) from a thread.
 *
 * @param input - Configuration including threadId, optional resourceId for scoping, and message limit
 * @param mastraAgent - The local Mastra agent instance with memory capabilities
 * @returns A snapshot containing thread messages and working memory, or an empty snapshot if:
 *   - The agent has no memory configured
 *   - The thread doesn't exist
 *   - An error occurs during loading
 *
 * @remarks
 * This function gracefully handles errors by returning an empty snapshot rather than throwing.
 * When resourceId is provided, it scopes the query to that specific resource for multi-tenant isolation.
 */
export async function loadAgentState(
  input: LoadAgentStateInput,
  mastraAgent: LocalMastraAgent,
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

    const thread = await memory.getThreadById({ threadId });

    if (!thread) {
      return emptySnapshot;
    }

    const queryResult = await memory.query({
      threadId,
      resourceId,
    });

    const mastraMessages = (queryResult.uiMessages || []) as MastraMemoryMessage[];
    const aguiMessages = mastraMsgsToAGUI(mastraMessages);

    let workingMemory: Record<string, any> | undefined;
    if (thread.metadata?.workingMemory) {
      if (typeof thread.metadata.workingMemory === "string") {
        try {
          workingMemory = JSON.parse(thread.metadata.workingMemory);
        } catch {
          // Invalid JSON, workingMemory will be undefined
          // but we still return messages
        }
      } else if (typeof thread.metadata.workingMemory === "object") {
        workingMemory = thread.metadata.workingMemory as Record<string, any>;
      }
    }

    return {
      threadsExist: aguiMessages.length > 0,
      agentId,
      resourceId,
      threadId,
      messages: aguiMessages,
      workingMemory,
    };
  } catch {
    return emptySnapshot;
  }
}
