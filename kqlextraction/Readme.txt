Requires .NET 6.0

> cd .\KqlExtraction\
> dotnet restore
> dotnet build -c Release
> .\KqlExtraction\bin\Release\net6.0\KqlExtraction.exe tests\test1.kql

{"FunctionCalls":["count","tostring","make_list","toreal"],"Joins":["rightsemi","leftouter"],"Operators":["where","extend","summarize","mv-expand","project-away","project"],"Tables":["SigninLogs"]}