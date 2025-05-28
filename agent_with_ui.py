import chainlit as cl
import semantic_kernel as sk
from semantic_kernel.connectors.ai.open_ai import AzureChatCompletion
from semantic_kernel.functions import kernel_function
from semantic_kernel.agents import ChatCompletionAgent, ChatHistoryAgentThread
from projects_data_plugin import ProjectsDataPlugin
from dotenv import load_dotenv
from utilities import Utilities

import os


@cl.on_chat_start
async def on_chat_start():

    load_dotenv()
    utilities = Utilities()
    projects_data_plugin = ProjectsDataPlugin()

    # Setup Semantic Kernel
    await projects_data_plugin.connect()
    database_schema_string = await projects_data_plugin.get_database_info()

    kernel = sk.Kernel()

    # Add your AI service (e.g., OpenAI)
    # Make sure OPENAI_API_KEY and OPENAI_ORG_ID are set in your environment
    service=AzureChatCompletion(
    endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
    
    api_key=os.getenv("AZURE_OPENAI_API_KEY")
)
    kernel.add_service(service)

    # Import the WeatherPlugin
    kernel.add_plugin(projects_data_plugin, plugin_name="ProjectsData")
    
    # Instantiate and add the Chainlit filter to the kernel
    # This will automatically capture function calls as Steps
    sk_filter = cl.SemanticKernelFilter(kernel=kernel)

    instructions = utilities.load_instructions("instructions.txt")

    # Replace the placeholder with the database schema string
    instructions = instructions.replace(
        "{database_schema_string}", database_schema_string)

    agent = ChatCompletionAgent(
        kernel=kernel,
        name="Host",
        instructions=instructions,
    )

    thread: ChatHistoryAgentThread = None
    cl.user_session.set("agent", agent)
    cl.user_session.set("thread", thread)

@cl.on_message
async def on_message(message: cl.Message):
    agent = cl.user_session.get("agent")
    thread = cl.user_session.get("thread")

    answer = cl.Message(content="")

    async for response in agent.invoke_stream(messages=message.content, thread=thread):

        if response.content:
            await answer.stream_token(str(response.content))

        thread = response.thread
        cl.user_session.set("thread", thread)

    # Send the final message
    await answer.send()