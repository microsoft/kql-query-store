using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using Kusto.Language.Utils;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Microsoft.Mstic.KqlQuery.Extraction
{
    public class KqlExtractionResult
    {
        public HashSet<string> FunctionCalls { get; set; } = new HashSet<string>();
        public Dictionary<string, HashSet<string>> Joins { get; set; } = new Dictionary<string, HashSet<string>>();
        public HashSet<string> Operators { get; set; } = new HashSet<string>();
        public HashSet<string> Tables { get; set; } = new HashSet<string>();
        // return input query ID here (apologies for Python naming)
        public string? query_id;

    }

    // Input is a list of these
    public class KqlQueryInput
    {
        public string? query_id;
        public string? query_text;
    }

    public class KqlExtraction
    {
        public static void Main(string[] args)
        {
            Console.WriteLine();
            Environment.ExitCode = new KqlExtraction().RunExtraction(args);
            Console.WriteLine();
        }

        private int RunExtraction(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("\nUsage: KqlExtraction path/to/query.kql\n");
                return 2;
            }

            var kqlFile = args[0];
            if (!File.Exists(kqlFile))
            {
                Console.WriteLine("\nUsage: KqlExtraction path/to/query.kql\n");
                return 2;
            }

            // read the file
            var kql = File.ReadAllText(kqlFile);
            // try to deserialize as JSON
            // if exception, treat as KqlQuery
            // else invoke code below on each KqlQueryInput.query_text
            // saving results to KqlExtractionResult with transferred query_id
            // write output (I guess we may need a second parameter for output file)


            var kqlExtractionResult = new KqlExtractionResult();

            try
            {
                var kql = File.ReadAllText(kqlFile);

                var kustoGlobals = GlobalState.Default.WithClusterList(Array.Empty<ClusterSymbol>());
                var kqlQuery = KustoCode.ParseAndAnalyze(kql, globals: kustoGlobals);

                var syntaxDiagnostics = kqlQuery.GetSyntaxDiagnostics();
                if (syntaxDiagnostics.Count > 0)
                {
                    Console.WriteLine("[!] Error: Syntax Error(s)");
                    foreach (var diagnostic in kqlQuery.GetSyntaxDiagnostics())
                    {
                        Console.WriteLine("  > [{0}:{1}] {2}", diagnostic.Start, diagnostic.End, diagnostic.Message);
                    }
                    return 1;
                }

                SyntaxElement.WalkNodes(kqlQuery.Syntax,
                    n =>
                    {
                        string? joinKind = null;
                        HashSet<string>? joinTarget = null;

                        if (n is FunctionCallExpression fc)
                        {
                            kqlExtractionResult.FunctionCalls.Add(fc.Name.SimpleName);
                        }
                        else if (n is NameReference nr)
                        {
                            if (nr.RawResultType.Kind == SymbolKind.Table)
                            {
                                kqlExtractionResult.Tables.Add(nr.Name.SimpleName);
                            }
                        }
                        else if (n.NameInParent == "Operator")
                        {
                            if (n is JoinOperator jo)
                            {
                                joinKind = "inner";
                                joinTarget = new HashSet<string>();

                                var kindParameter = jo.Parameters.Where(p => p.Name.SimpleName == "kind");
                                if (kindParameter.Count() == 1)
                                {
                                    joinKind = kindParameter.First().Expression.ToString();
                                }

                                if (jo.Expression is NameReference jonr)
                                {
                                    joinTarget.Add(jonr.SimpleName);
                                }
                                else if (jo.Expression is ParenthesizedExpression jopr)
                                {
                                    if (jopr.Expression is NameReference joprnr)
                                    {
                                        joinTarget.Add(joprnr.SimpleName);
                                    }
                                }

                                if (joinTarget.Count() == 0)
                                {
                                    joinTarget.Add("(...)");
                                }
                            }
                            else if (n is LookupOperator lo)
                            {
                                joinKind = "leftouter";
                                joinTarget = new HashSet<string>();

                                if (lo.Expression is NameReference lonr)
                                {
                                    joinTarget.Add(lonr.SimpleName);
                                }
                                else if (lo.Expression is ParenthesizedExpression lopr)
                                {
                                    if (lopr.Expression is NameReference loprnr)
                                    {
                                        joinTarget.Add(loprnr.SimpleName);
                                    }
                                }

                                if (joinTarget.Count() == 0)
                                {
                                    joinTarget.Add("(...)");
                                }
                            }
                            else
                            {
                                kqlExtractionResult.Operators.Add(n.GetFirstToken().Text);
                            }
                        }
                        else if (n is UnionOperator uo)
                        {
                            joinKind = "union";
                            joinTarget = new HashSet<string>();

                            foreach(var t in uo.Expressions)
                            {
                                if (t.Element is NameReference uonr)
                                {
                                    joinTarget.Add(uonr.SimpleName);
                                }
                            }
                        }

                        if ((joinKind != null) && (joinTarget != null))
                        {
                            if (!kqlExtractionResult.Joins.ContainsKey(joinKind))
                            {
                                kqlExtractionResult.Joins[joinKind] = new HashSet<string>();
                            }
                            kqlExtractionResult.Joins[joinKind].AddRange(joinTarget);
                        }
                    });
            }
            catch (Exception ex)
            {
                Console.WriteLine("[!] Error: Exception '{0}'", ex.Message);
                return 2;
            }


            Console.WriteLine(JsonSerializer.Serialize(kqlExtractionResult));

            return 0;
        }
    }
}
