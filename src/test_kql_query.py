from .kql_query import KqlQuery

def test_kql_query():
    kql = KqlQuery(
        source_path="https://github.com/a/b/file.kql",
        query="SecurityAlert | take 1"
    )
    print(kql)
    print(kql.asdict())
    print(kql.to_json())

    KqlQuery.kql_list_to_pylist([kql, kql])

    KqlQuery.kql_list_to_json([kql, kql])

    KqlQuery.kql_list_to_df([kql, kql])



