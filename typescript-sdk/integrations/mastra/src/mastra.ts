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

export interface MastraAgentConfig extends AgentConfig {
  agent: LocalMastraAgent | RemoteMastraAgent;
  resourceId?: string;
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

    // Create LangGraph-compatible client interface for CopilotKit
    if (this.isLocalMastraAgent(agent)) {
      this.client = {
        threads: {
          getState: async (threadId: string) => {
            console.info(`[MastraAgent.client] Loading state for thread ${threadId}`);

            if (!this.resourceId) {
              console.warn(
                `[MastraAgent.client] Cannot load thread state without resourceId. ` +
                `Agent: ${this.agentId}, Thread: ${threadId}. Returning empty state.`
              );
              return { values: { messages: [] } };
            }

            const stateSnapshot = await loadAgentState(
              {
                agentId: this.agentId!,
                resourceId: this.resourceId,
                threadId,
                limit: 100,
              },
              agent
            );

            console.info(`[MastraAgent.client] State loaded: threadsExist=${stateSnapshot.threadsExist}, messages=${stateSnapshot.messages.length}`);

            // Convert AG-UI messages to LangChain format for CopilotKit
            const { aguiMessagesToLangChain } = await import("./utils/messages.js");
            const langChainMessages = aguiMessagesToLangChain(stateSnapshot.messages);

            console.info(`[MastraAgent.client] Converted to LangChain format:`, JSON.stringify(langChainMessages).substring(0, 200));

            const returnValue = {
              values: {
                messages: langChainMessages,
                ...stateSnapshot.workingMemory,
              },
            };

            console.info(`[MastraAgent.client] Return value:`, JSON.stringify(returnValue).substring(0, 500));

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
          if (!this.resourceId) {
            console.warn(
              `[MastraAgent] Cannot load thread state without resourceId. ` +
              `Agent: ${this.agentId}, Thread: ${input.threadId}. Skipping state hydration.`
            );
          } else {
            try {
              console.info(
                `[MastraAgent] Loading thread state for threadId: ${input.threadId}, resourceId: ${this.resourceId}`
              );

              const stateSnapshot = await loadAgentState(
                {
                  agentId: this.agentId!,
                  resourceId: this.resourceId,
                  threadId: input.threadId,
                  limit: 100,
                },
                this.agent
              );

              console.info(
                `[MastraAgent] loadAgentState result: threadsExist=${stateSnapshot.threadsExist}, messages=${stateSnapshot.messages.length}, hasWorkingMemory=${!!stateSnapshot.workingMemory}`
              );

              if (stateSnapshot.threadsExist && stateSnapshot.messages.length > 0) {
                const messagesSnapshotEvent: MessagesSnapshotEvent = {
                  type: EventType.MESSAGES_SNAPSHOT,
                  messages: stateSnapshot.messages as Message[],
                };
                subscriber.next(messagesSnapshotEvent);

                console.info(
                  `[MastraAgent] Loaded ${stateSnapshot.messages.length} historical messages for thread ${input.threadId}`
                );
              } else {
                console.info(
                  `[MastraAgent] No historical messages to load for thread ${input.threadId}`
                );
              }

              if (stateSnapshot.workingMemory && Object.keys(stateSnapshot.workingMemory).length > 0) {
                const stateSnapshotEvent: StateSnapshotEvent = {
                  type: EventType.STATE_SNAPSHOT,
                  snapshot: stateSnapshot.workingMemory,
                };
                subscriber.next(stateSnapshotEvent);

                console.info(
                  `[MastraAgent] Restored working memory for thread ${input.threadId}`
                );
              }
            } catch (error) {
              console.error(
                `[MastraAgent] Failed to load thread history for ${input.threadId}:`,
                error
              );
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
                resourceId: this.resourceId ?? input.threadId,
                createdAt: new Date(),
                updatedAt: new Date(),
              };
            }

            const existingMemory = JSON.parse((thread.metadata?.workingMemory as string) ?? "{}");
            const { messages, ...rest } = input.state;
            const workingMemory = JSON.stringify({ ...existingMemory, ...rest });

            // Update thread metadata with new working memory
            await memory.saveThread({
              thread: {
                ...thread,
                metadata: {
                  ...thread.metadata,
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

              const argsEvent: ToolCallArgsEvent = {
                type: EventType.TOOL_CALL_ARGS,
                toolCallId: streamPart.toolCallId,
                delta: JSON.stringify(streamPart.args),
              };
              subscriber.next(argsEvent);

              const endEvent: ToolCallEndEvent = {
                type: EventType.TOOL_CALL_END,
                toolCallId: streamPart.toolCallId,
              };
              subscriber.next(endEvent);
            },
            onToolResultPart(streamPart) {
              const toolCallResultEvent: ToolCallResultEvent = {
                type: EventType.TOOL_CALL_RESULT,
                toolCallId: streamPart.toolCallId,
                content: JSON.stringify(streamPart.result),
                messageId: randomUUID(),
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
                    try {
                      console.info(
                        `[MastraAgent] Querying final messages for thread ${input.threadId} with resourceId: ${this.resourceId}`
                      );

                      const { uiMessages } = await memory.query({
                        threadId: input.threadId,
                        resourceId: this.resourceId,
                      });

                      console.info(
                        `[MastraAgent] Query returned ${uiMessages?.length || 0} uiMessages`
                      );

                      if (uiMessages && uiMessages.length > 0) {
                        const { mastraMsgsToAGUI } = await import("./utils/messages.js");
                        const aguiMessages = mastraMsgsToAGUI(uiMessages as any);

                        const messagesSnapshotEvent: MessagesSnapshotEvent = {
                          type: EventType.MESSAGES_SNAPSHOT,
                          messages: aguiMessages as Message[],
                        };
                        subscriber.next(messagesSnapshotEvent);

                        console.info(
                          `[MastraAgent] Emitted final message snapshot with ${aguiMessages.length} messages for thread ${input.threadId}`
                        );
                      } else {
                        console.info(
                          `[MastraAgent] No messages to emit in final snapshot for thread ${input.threadId}`
                        );
                      }
                    } catch (error) {
                      console.error(`[MastraAgent] Failed to emit final message snapshot:`, error);
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
      console.info(`[MastraAgent.getNewMessages] No threadId, returning all ${messages.length} messages`);
      return messages;
    }

    if (!this.isLocalMastraAgent(this.agent)) {
      console.info(`[MastraAgent.getNewMessages] Remote agent, returning all ${messages.length} messages`);
      return messages;
    }

    try {
      const memory = await this.agent.getMemory();
      if (!memory) {
        console.info(`[MastraAgent.getNewMessages] No memory, returning all ${messages.length} messages`);
        return messages;
      }

      console.info(
        `[MastraAgent.getNewMessages] Querying existing messages for thread ${threadId} with resourceId: ${this.resourceId}`
      );

      const { uiMessages: existingMessages } = await memory.query({
        threadId,
        resourceId: this.resourceId,
      });

      console.info(
        `[MastraAgent.getNewMessages] Found ${existingMessages?.length || 0} existing messages in thread`
      );

      const existingIds = new Set(existingMessages.map((m: any) => m.id));

      const newMessages = messages.filter((msg) => !existingIds.has(msg.id));

      console.info(
        `[MastraAgent.getNewMessages] Filtered ${messages.length} input messages to ${newMessages.length} new messages for thread ${threadId}`
      );

      if (messages.length > 0 && newMessages.length === 0) {
        console.warn(
          `[MastraAgent.getNewMessages] All ${messages.length} messages already exist in thread - this may indicate duplicate message IDs`
        );
      }

      return newMessages;
    } catch (error) {
      console.error(`[MastraAgent.getNewMessages] Failed to filter messages, sending all:`, error);
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
    // Ensure we have a resourceId - it MUST be provided for memory to work
    // According to Mastra docs: "agents won't store or recall information unless both thread and resource are provided"
    if (!this.resourceId) {
      console.warn(
        `[MastraAgent] No resourceId configured! Memory will not persist. ThreadId: ${threadId}`
      );
    }
    const resourceId = this.resourceId ?? threadId;

    const messagesToSend = await this.getNewMessages({ threadId, messages });
    const convertedMessages = convertAGUIMessagesToMastra(messagesToSend);
    this.runtimeContext?.set('ag-ui', { context: inputContext });
    const runtimeContext = this.runtimeContext;

    if (this.isLocalMastraAgent(this.agent)) {
      // Local agent - use the agent's stream method directly
      try {
        const response = await this.agent.stream(convertedMessages, {
          threadId,
          resourceId,
          runId,
          clientTools,
          runtimeContext,
        });

        // For local agents, the response should already be a stream
        // Process it using the agent's built-in streaming mechanism
        if (response && typeof response === "object") {
          // If the response has a toDataStreamResponse method, use it
          if (
            "toDataStreamResponse" in response &&
            typeof response.toDataStreamResponse === "function"
          ) {
            const dataStreamResponse = response.toDataStreamResponse();
            if (dataStreamResponse && dataStreamResponse.body) {
              await processDataStream({
                stream: dataStreamResponse.body,
                onTextPart,
                onToolCallPart,
                onToolResultPart,
                onFinishMessagePart,
              });
              await onRunFinished?.();
            } else {
              throw new Error("Invalid data stream response from local agent");
            }
          } else {
            // If it's already a readable stream, process it directly
            await processDataStream({
              stream: response as any,
              onTextPart,
              onToolCallPart,
              onToolResultPart,
              onFinishMessagePart,
            });
            await onRunFinished?.();
          }
        } else {
          throw new Error("Invalid response from local agent");
        }
      } catch (error) {
        onError?.(error as Error);
      }
    } else {
      // Remote agent - use the remote agent's stream method
      try {
        const response = await this.agent.stream({
          threadId,
          resourceId,
          runId,
          messages: convertedMessages,
          clientTools,
        });

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
