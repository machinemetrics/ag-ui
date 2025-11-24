import type {
  AgentConfig,
  BaseEvent,
  Message,
  MessagesSnapshotEvent,
  RunAgentInput,
  RunFinishedEvent,
  RunStartedEvent,
  StateSnapshotEvent,
  TextMessageChunkEvent,
  ToolCallArgsEvent,
  ToolCallEndEvent,
  ToolCallResultEvent,
  ToolCallStartEvent,
} from "@ag-ui/client";
import { AbstractAgent, EventType } from "@ag-ui/client";
import { processDataStream } from "@ai-sdk/ui-utils";
import type { StorageThreadType } from "@mastra/core";
import { Agent as LocalMastraAgent } from "@mastra/core/agent";
import { RuntimeContext } from "@mastra/core/runtime-context";
import { randomUUID } from "crypto";
import { Observable } from "rxjs";
import { MastraClient } from "@mastra/client-js";
import { toAISdkFormat } from "@mastra/ai-sdk";
import { createUIMessageStream, createUIMessageStreamResponse } from "ai";
type RemoteMastraAgent = ReturnType<MastraClient["getAgent"]>;
import {
  convertAGUIMessagesToMastra,
  GetLocalAgentsOptions,
  getLocalAgents,
  getRemoteAgents,
  GetRemoteAgentsOptions,
  GetLocalAgentOptions,
  getLocalAgent,
  GetNetworkOptions,
  getNetwork,
} from "./utils";
import { loadAgentState } from "./server/loadAgentState.js";
import { aguiMessagesToLangChain, mastraMsgsToAGUI } from "./utils/messages.js";

/**
 * Configuration for creating a MastraAgent
 */
export interface MastraAgentConfig extends AgentConfig {
  /** The Mastra agent instance (local or remote) */
  agent: LocalMastraAgent | RemoteMastraAgent;
  /**
   * Resource identifier for scoping thread data to a specific user, tenant, or group.
   *
   * @remarks
   * - **With resourceId**: Thread history and state are scoped to this resource for multi-tenant isolation
   * - **Without resourceId**: Agent operates in stateless mode - no thread history is loaded or persisted
   *
   * @example
   * ```typescript
   * // Multi-tenant setup (recommended)
   * const agent = new MastraAgent({
   *   agent: mastraAgent,
   *   resourceId: "user-123", // Scopes to specific user
   * });
   *
   * // Stateless mode (no history)
   * const agent = new MastraAgent({
   *   agent: mastraAgent,
   *   // resourceId omitted - no thread persistence
   * });
   * ```
   */
  resourceId?: string;
  /** Optional runtime context for passing additional data to the agent */
  runtimeContext?: RuntimeContext;
}

interface MastraAgentStreamOptions {
  onTextPart?: (text: string) => void;
  onFinishMessagePart?: () => void;
  onToolCallPart?: (streamPart: { toolCallId: string; toolName: string; args: any }) => void;
  onToolResultPart?: (streamPart: { toolCallId: string; result: any }) => void;
  onError?: (error: Error) => void;
  onRunFinished?: () => Promise<void>;
}

export class MastraAgent extends AbstractAgent {
  agent: LocalMastraAgent | RemoteMastraAgent;
  resourceId?: string;
  runtimeContext?: RuntimeContext;
  client?: {
    threads: {
      getState(threadId: string): Promise<{ values: Record<string, any> }>;
    };
  };

  constructor({ agent, resourceId, runtimeContext, ...rest }: MastraAgentConfig) {
    super(rest);
    this.agent = agent;
    this.resourceId = resourceId;
    this.runtimeContext = runtimeContext ?? new RuntimeContext();
    // Create LangGraph-compatible client interface for CopilotKit compatibility.
    // CopilotKit calls agent.client.threads.getState() before run() to load previous
    // conversation state for rehydration. This is separate from the state loading that
    // happens within run() which emits MESSAGES_SNAPSHOT and STATE_SNAPSHOT events.
    if (this.isLocalMastraAgent(agent)) {
      this.client = {
        threads: {
          getState: async (threadId: string) => {
            if (!this.resourceId) {
              return { values: { messages: [] } };
            }

            const stateSnapshot = await loadAgentState(
              {
                agentId: this.agentId!,
                resourceId: this.resourceId,
                threadId,
                limit: 100,
              },
              agent,
            );

            // Convert AG-UI messages to LangChain format for CopilotKit
            const langChainMessages = aguiMessagesToLangChain(stateSnapshot.messages);

            const returnValue = {
              values: {
                messages: langChainMessages,
                ...stateSnapshot.workingMemory,
              },
            };

            return returnValue;
          },
        },
      };
    }
  }

  protected run(input: RunAgentInput): Observable<BaseEvent> {
    let messageId = randomUUID();

    return new Observable<BaseEvent>((subscriber) => {
      const run = async () => {
        const runStartedEvent: RunStartedEvent = {
          type: EventType.RUN_STARTED,
          threadId: input.threadId,
          runId: input.runId,
        };

        subscriber.next(runStartedEvent);

        // Load thread history if threadId present and using local agent
        if (input.threadId && this.isLocalMastraAgent(this.agent)) {
          if (this.resourceId) {
            const stateSnapshot = await loadAgentState(
              {
                agentId: this.agentId!,
                resourceId: this.resourceId,
                threadId: input.threadId,
                limit: 100,
              },
              this.agent,
            );

            if (stateSnapshot.threadsExist && stateSnapshot.messages.length > 0) {
              const messagesSnapshotEvent: MessagesSnapshotEvent = {
                type: EventType.MESSAGES_SNAPSHOT,
                messages: stateSnapshot.messages as Message[],
              };
              subscriber.next(messagesSnapshotEvent);
            }

            if (
              stateSnapshot.workingMemory &&
              Object.keys(stateSnapshot.workingMemory).length > 0
            ) {
              const stateSnapshotEvent: StateSnapshotEvent = {
                type: EventType.STATE_SNAPSHOT,
                snapshot: stateSnapshot.workingMemory,
              };
              subscriber.next(stateSnapshotEvent);
            }
          }
        }

        // Handle local agent memory management (from Mastra implementation)
        if (this.isLocalMastraAgent(this.agent)) {
          const memory = await this.agent.getMemory();

          if (memory && input.state && Object.keys(input.state || {}).length > 0) {
            let thread: StorageThreadType | null = await memory.getThreadById({
              threadId: input.threadId,
            });

            if (!thread) {
              thread = {
                id: input.threadId,
                title: "",
                metadata: {},
                createdAt: new Date(),
                updatedAt: new Date(),
                resourceId: this.resourceId!,
              };
            }

            const existingMemory = JSON.parse((thread!.metadata?.workingMemory as string) ?? "{}");
            const { messages, ...rest } = input.state;
            const workingMemory = JSON.stringify({ ...existingMemory, ...rest });

            // Update thread metadata with new working memory
            await memory.saveThread({
              thread: {
                ...thread!,
                metadata: {
                  ...thread!.metadata,
                  workingMemory,
                },
              },
            });
          }
        }

        try {
          await this.streamMastraAgent(input, {
            onTextPart: (text) => {
              const event: TextMessageChunkEvent = {
                type: EventType.TEXT_MESSAGE_CHUNK,
                role: "assistant",
                messageId,
                delta: text,
              };
              subscriber.next(event);
            },
            onToolCallPart: (streamPart) => {
              const startEvent: ToolCallStartEvent = {
                type: EventType.TOOL_CALL_START,
                parentMessageId: messageId,
                toolCallId: streamPart.toolCallId,
                toolCallName: streamPart.toolName,
              };
              subscriber.next(startEvent);

              // Ensure args is always an object, even if undefined
              const args = streamPart.args !== undefined ? streamPart.args : {};
              const argsEvent: ToolCallArgsEvent = {
                type: EventType.TOOL_CALL_ARGS,
                toolCallId: streamPart.toolCallId,
                delta: JSON.stringify(args),
              };
              subscriber.next(argsEvent);

              const endEvent: ToolCallEndEvent = {
                type: EventType.TOOL_CALL_END,
                toolCallId: streamPart.toolCallId,
              };
              subscriber.next(endEvent);
            },
            onToolResultPart(streamPart) {
              const resultMessageId = randomUUID();

              const toolCallResultEvent: ToolCallResultEvent = {
                type: EventType.TOOL_CALL_RESULT,
                toolCallId: streamPart.toolCallId,
                content: JSON.stringify(streamPart.result),
                messageId: resultMessageId,
                role: "tool",
              };

              subscriber.next(toolCallResultEvent);
            },
            onFinishMessagePart: async () => {
              messageId = randomUUID();
            },
            onError: (error) => {
              console.error("error", error);
              // Handle error
              subscriber.error(error);
            },
            onRunFinished: async () => {
              if (this.isLocalMastraAgent(this.agent)) {
                try {
                  const memory = await this.agent.getMemory();
                  if (memory) {
                    const workingMemory = await memory.getWorkingMemory({
                      threadId: input.threadId,
                      memoryConfig: {
                        workingMemory: {
                          enabled: true,
                        },
                      },
                    });

                    if (typeof workingMemory === "string") {
                      const snapshot = JSON.parse(workingMemory);

                      if (snapshot && !("$schema" in snapshot)) {
                        const stateSnapshotEvent: StateSnapshotEvent = {
                          type: EventType.STATE_SNAPSHOT,
                          snapshot,
                        };

                        subscriber.next(stateSnapshotEvent);
                      }
                    }

                    // Emit MESSAGES_SNAPSHOT with complete message list (matches LangGraph pattern)
                    const { uiMessages } = await memory.query({
                      threadId: input.threadId,
                      resourceId: this.resourceId,
                    });

                    if (uiMessages && uiMessages.length > 0) {
                      const aguiMessages = mastraMsgsToAGUI(uiMessages as any);

                      const messagesSnapshotEvent: MessagesSnapshotEvent = {
                        type: EventType.MESSAGES_SNAPSHOT,
                        messages: aguiMessages as Message[],
                      };
                      subscriber.next(messagesSnapshotEvent);
                    }
                  }
                } catch (error) {
                  console.error("Error sending state snapshot", error);
                }
              }

              // Emit run finished event
              subscriber.next({
                type: EventType.RUN_FINISHED,
                threadId: input.threadId,
                runId: input.runId,
              } as RunFinishedEvent);

              // Complete the observable
              subscriber.complete();
            },
          });
        } catch (error) {
          console.error("Stream error:", error);
          subscriber.error(error);
        }
      };

      run();

      return () => {};
    });
  }

  isLocalMastraAgent(agent: LocalMastraAgent | RemoteMastraAgent): agent is LocalMastraAgent {
    return "getMemory" in agent;
  }

  private async getNewMessages({
    threadId,
    messages,
  }: {
    threadId?: string;
    messages: Message[];
  }): Promise<Message[]> {
    if (!threadId) {
      return messages;
    }

    if (!this.isLocalMastraAgent(this.agent)) {
      return messages;
    }

    const memory = await this.agent.getMemory();
    if (!memory) {
      return messages;
    }

    try {
      const { uiMessages: existingMessages } = await memory.query({
        threadId,
        resourceId: this.resourceId,
      });

      const existingIds = new Set(existingMessages.map((m: any) => m.id));

      const newMessages = messages.filter((msg) => !existingIds.has(msg.id));

      return newMessages;
    } catch {
      return messages;
    }
  }

  /**
   * Streams in process or remote mastra agent.
   * @param input - The input for the mastra agent.
   * @param options - The options for the mastra agent.
   * @returns The stream of the mastra agent.
   */
  private async streamMastraAgent(
    { threadId, runId, messages, tools, context: inputContext }: RunAgentInput,
    {
      onTextPart,
      onFinishMessagePart,
      onToolCallPart,
      onToolResultPart,
      onError,
      onRunFinished,
    }: MastraAgentStreamOptions,
  ): Promise<void> {
    const clientTools = tools.reduce(
      (acc, tool) => {
        acc[tool.name as string] = {
          id: tool.name,
          description: tool.description,
          inputSchema: tool.parameters,
        };
        return acc;
      },
      {} as Record<string, any>,
    );
    const resourceId = this.resourceId;

    const messagesToSend = await this.getNewMessages({ threadId, messages });
    const convertedMessages = convertAGUIMessagesToMastra(messagesToSend);
    this.runtimeContext?.set("ag-ui", { context: inputContext });
    const runtimeContext = this.runtimeContext;

    if (this.isLocalMastraAgent(this.agent)) {
      // Local agent - use the agent's stream method directly
      try {
        // Base stream options without thread/resource parameters
        const baseStreamOptions = {
          runId,
          clientTools,
          runtimeContext,
        };

        // Only include threadId and resourceId if both are available
        const streamOptions =
          threadId && resourceId
            ? { ...baseStreamOptions, threadId, resourceId }
            : baseStreamOptions;

        const mastraStream = await this.agent.stream(convertedMessages, streamOptions);

        // Iterate over fullStream and handle each part
        for await (const part of mastraStream.fullStream) {
          switch (part.type) {
            case 'text-delta':
              await onTextPart?.(part.textDelta);
              break;
            case 'step-start':
              // Step started - no text content to emit
              break;
            case 'step-finish': {
              // Step finished - contains the thinking text for this step
              const stepPart = part as any;
              const payload = stepPart.payload;
              if (payload?.output?.text) {
                // Emit the thinking text as text deltas
                await onTextPart?.(payload.output.text);
              }
              break;
            }
            case 'tool-call': {
              // Tool call data is nested in payload at runtime
              const toolCallPart = part as any;
              const payload = toolCallPart.payload || toolCallPart;
              await onToolCallPart?.({
                toolCallId: payload.toolCallId,
                toolName: payload.toolName,
                args: payload.args !== undefined ? payload.args : {},
              });
              break;
            }
            case 'tool-result': {
              // Tool result data is nested in payload at runtime
              const toolResultPart = part as any;
              const payload = toolResultPart.payload || toolResultPart;
              await onToolResultPart?.({
                toolCallId: payload.toolCallId,
                result: payload.result,
              });
              break;
            }
            case 'finish':
              await onFinishMessagePart?.();
              break;
            case 'error':
              throw new Error(String(part.error));
          }
        }
        await onRunFinished?.();
      } catch (error) {
        onError?.(error as Error);
      }
    } else {
      // Remote agent - use the remote agent's stream method
      try {
        // Base stream options without thread/resource parameters
        const baseStreamOptions = {
          runId,
          messages: convertedMessages,
          clientTools,
        };

        // Only include threadId and resourceId if both are available
        const streamOptions =
          threadId && resourceId
            ? { ...baseStreamOptions, threadId, resourceId }
            : baseStreamOptions;

        const response = await this.agent.stream(streamOptions);

        // Remote agents should have a processDataStream method
        if (response && typeof response.processDataStream === "function") {
          await response.processDataStream({
            onTextPart,
            onToolCallPart,
            onToolResultPart,
            onFinishMessagePart,
          });
          await onRunFinished?.();
        } else {
          throw new Error("Invalid response from remote agent");
        }
      } catch (error) {
        onError?.(error as Error);
      }
    }
  }

  static async getRemoteAgents(
    options: GetRemoteAgentsOptions,
  ): Promise<Record<string, AbstractAgent>> {
    return getRemoteAgents(options);
  }

  static getLocalAgents(options: GetLocalAgentsOptions): Record<string, AbstractAgent> {
    return getLocalAgents(options);
  }

  static getLocalAgent(options: GetLocalAgentOptions) {
    return getLocalAgent(options);
  }

  static getNetwork(options: GetNetworkOptions) {
    return getNetwork(options);
  }
}
