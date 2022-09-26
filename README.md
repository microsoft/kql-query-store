# Interactive KQL Query Store

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://aka.ms/kql-query-store)

Currently many KQL queries are published on GitHub by Microsoft and Security Community on GitHub. All the queries are scattered as unstructured data and disorganized in various places making it difficult to discover for defenders and detection authors. 

GitHub search interface is not flexible to satisfy various custom search needs for defenders to effectively search various KQL queries by datasource , KQL operators , parsing of complex fields in data sources, custom tags if available etc. Having it easy to discover will help defenders in referencing existing work while writing new queries, reuse complex parsing examples in specific data sources and much more. 

## Project Goals

- Organized data store of KQL queries as a structured data store
- Easy discoverability of KQL Queries based on tags, KQL operators, Datasource etc. 
- Point to relevant sources and GitHub links. 
- Interactive dashboard to explore the structured data.
- Insights on various KQL queries from Azure Sentinel

## Architecture
![raw_image](https://raw.github.com/microsoft/kql-query-store/master/images/DataFlowDiagram.png)


## Docker instruction
if you wish to host this locally/in-house, you can use below instructions to build docker images and host it. For more detailed instructions, check out Streamlit docs. [Deploy Streamlit using Docker](https://docs.streamlit.io/knowledge-base/tutorials/deploy/docker)

Build image

`docker build -t kql-query-store .`

Run the docker container

`docker run -p 8501:8501 kql-query-store`

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.