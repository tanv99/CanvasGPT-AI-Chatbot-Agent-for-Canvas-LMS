from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.client import User
from diagrams.custom import Custom
from diagrams.programming.language import Python
from diagrams.onprem.container import Docker
from diagrams.onprem.ci import GitlabCI
from diagrams.onprem.vcs import Git
from diagrams.gcp.compute import GCE

with Diagram("Final Project Architecture", show=False, direction="LR"):
    # Block 1: User, Canvas, Chrome Extension
    with Cluster("User Interaction"):
        user = User("User")
        canvas = Custom("Canvas", "images/canvas_api.png")
        chrome_extension = Custom("Chrome Extension", "images/chrome_extension.png")

        user >> Edge(label="Interacts") >> canvas
        canvas >> Edge(label="Extension Data") >> chrome_extension

    # Block 2: FastAPI, Langraph
    with Cluster("Backend API"):
        fastapi = Custom("FastAPI", "images/fastapi.png")
        langraph = Custom("(Langraph)", "images/langraph.png")

        chrome_extension >> Edge(label="Sends Requests") >> fastapi

    # Block 3: AI Components
    with Cluster("AI Components"):
        llama_module = Custom("Llama Module", "images/llma.png")
        rag = Custom("RAG", "images/llm.png")
        meeting_schedule = Custom("Meeting Schedule", "images/meeting.png")
        doc_handler = Custom("Document Handler", "images/document.png")
        websearch_agent = Custom("Web Search (Agent)", "images/tavily.png")
        canvas_api = Custom("Canvas API", "images/canvas_api.png")

        fastapi << Edge(label="Processes via Langraph") >> langraph
        langraph >> [rag, meeting_schedule, doc_handler, websearch_agent, canvas_api]

    # Block 4: Pinecone
    with Cluster("Vector Database"):
        pinecone = Custom("Pinecone", "images/pinecone.png")
        llama_module >> Edge(label="Stores Vectors") >> pinecone

    # Block 5: Deployment Pipeline
    with Cluster("Deployment Pipeline"):
        git = Git("Git")
        gitaction = GitlabCI("GitAction")
        docker = Docker("Docker")
        gcp_vm = GCE("GCP VM")

        git >> Edge(label="Triggers") >> gitaction
        gitaction >> Edge(label="Builds") >> docker
        docker >> Edge(label="Deploys") >> gcp_vm

    # Connecting AI Components to Deployment
    llama_module >> Edge(label="Uses") >> docker
