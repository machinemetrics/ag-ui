import type { Message } from "@ag-ui/client";
import type { CoreMessage } from "@mastra/core";

export interface AGUIMessage {
  id: string;
  role: "user" | "assistant" | "system" | "tool";
  content: string;
  toolCalls?: Array<{
    id: string;
    type: "function";
    function: {
      name: string;
      arguments: string;
    };
  }>;
  toolCallId?: string;
}

export interface MastraMemoryMessage {
  id: string;
  role: "user" | "assistant" | "system" | "tool";
  content: string | string[] | Array<{ type: string; text?: string; [key: string]: any }>;
  createdAt: Date;
  metadata?: Record<string, any>;
}

export function mastraMsgToAGUI(mastraMessage: MastraMemoryMessage): AGUIMessage {
  const { id, role, content, metadata } = mastraMessage;

  // Ensure we always have an ID - critical for CopilotKit GraphQL serialization
  if (!id) {
    throw new Error(`Message missing required 'id' field. Role: ${role}`);
  }

  let textContent = "";
  let toolCalls: AGUIMessage["toolCalls"] = undefined;
  let toolCallId: string | undefined = undefined;

  if (typeof content === "string") {
    textContent = content;
  } else if (Array.isArray(content)) {
    const textParts: string[] = [];

    for (const part of content) {
      if (typeof part === "string") {
        textParts.push(part);
      } else if (part && typeof part === "object") {
        if (part.type === "text" && part.text) {
          textParts.push(part.text);
        } else if (part.type === "tool-call") {
          if (!toolCalls) toolCalls = [];
          toolCalls.push({
            id: part.toolCallId || part.id || `tool-${Date.now()}`,
            type: "function",
            function: {
              name: part.toolName || part.name || "unknown",
              arguments: JSON.stringify(part.args || part.arguments || {}),
            },
          });
        } else if (part.type === "tool-result") {
          toolCallId = part.toolCallId || part.id;
          textContent = typeof part.result === "string"
            ? part.result
            : JSON.stringify(part.result);
        }
      }
    }

    textContent = textParts.join("");
  } else if (content && typeof content === "object") {
    textContent = JSON.stringify(content);
  }

  const aguiMessage: AGUIMessage = {
    id,
    role,
    content: textContent,
  };

  if (toolCalls && toolCalls.length > 0) {
    aguiMessage.toolCalls = toolCalls;
  }

  if (toolCallId) {
    aguiMessage.toolCallId = toolCallId;
  }

  return aguiMessage;
}

export function mastraMsgsToAGUI(mastraMessages: MastraMemoryMessage[]): AGUIMessage[] {
  const aguiMessages: AGUIMessage[] = [];

  for (const mastraMsg of mastraMessages) {
    try {
      aguiMessages.push(mastraMsgToAGUI(mastraMsg));
    } catch (error) {
      // Log error for debugging but continue processing other messages
      console.error('[mastraMsgsToAGUI] Error converting message:', error, 'Message:', mastraMsg);
    }
  }

  return aguiMessages;
}

/**
 * LangChain message format (what CopilotKit expects)
 */
export interface LangChainMessage {
  id?: string;
  type: "human" | "ai" | "system" | "tool";
  content: string | Array<any>;
  tool_calls?: Array<{
    id: string;
    name: string;
    args: Record<string, any>;
  }>;
  tool_call_id?: string;
}

/**
 * Convert AG-UI messages to LangChain format for CopilotKit
 *
 * CopilotKit's runtime expects LangChain-formatted messages with:
 * - type: "human" | "ai" | "system" | "tool"
 * - content: string or array
 * - tool_calls: array of {id, name, args}
 */
export function aguiMessagesToLangChain(messages: AGUIMessage[]): LangChainMessage[] {
  return messages.map((msg) => {
    const langChainMsg: LangChainMessage = {
      id: msg.id,
      type: msg.role === "user" ? "human" : msg.role === "assistant" ? "ai" : msg.role as any,
      content: msg.content || "",
    };

    // AI messages must always have tool_calls array (even if empty)
    if (msg.role === "assistant") {
      langChainMsg.tool_calls = (msg.toolCalls || [])
        .map((tc) => {
          try {
            return {
              id: tc.id,
              name: tc.function.name,
              args: JSON.parse(tc.function.arguments),
            };
          } catch {
            return null;
          }
        })
        .filter((tc): tc is NonNullable<typeof tc> => tc !== null);
    }

    // Add tool_call_id for tool messages
    if (msg.toolCallId) {
      langChainMsg.tool_call_id = msg.toolCallId;
    }

    return langChainMsg;
  });
}
