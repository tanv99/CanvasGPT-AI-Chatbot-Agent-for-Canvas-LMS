from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.onprem.container import Docker
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.compute import Server
from diagrams.aws.storage import S3

with Diagram("Data Ingestion Flow", show=False, direction="LR"):
    # Data Sources
    with Cluster("Data Sources"):
        website1 = Server("Website\n(Scrape Data)")

    # Airflow and Storage
    with Cluster("Data Processing"):
        airflow = Airflow("Apache Airflow")
        s3_bucket = S3("AWS S3 Bucket")
        snowflake = Custom("Snowflake", "images/snowflake.png")
        pinecone = Custom("Pinecone", "images/pinecone.png")

    # Data Flow for Scraped Data
    website1 >> Edge(label="Scrape Data") >> airflow
    airflow >> Edge(label="Store Data") >> s3_bucket

    # Data Flow for Snowflake Data
    website1 >> Edge(label="Extract Data") >> airflow
    airflow >> Edge(label="Load Data") >> snowflake
    snowflake >> Edge(label="Embeddings") >> pinecone