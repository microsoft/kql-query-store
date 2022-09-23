using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using Kusto.Language.Utils;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Microsoft.Mstic.KqlQuery.Extraction
{
    public class KqlExtractionResult
    {
        public string Id { get; set; } = "";
        public HashSet<string> FunctionCalls { get; set; } = new HashSet<string>();
        public Dictionary<string, HashSet<string>> Joins { get; set; } = new Dictionary<string, HashSet<string>>();
        public HashSet<string> Operators { get; set; } = new HashSet<string>();
        public HashSet<string> Tables { get; set; } = new HashSet<string>();
    }

    public class KqlExtraction
    {
        public static void Main(string[] args)
        {
            string? l = null;
            while ((l = Console.ReadLine()) != null)
            {
                var kqlQuery = l.Split(',', 2);

                var kqlExtractionResult = new KqlExtractionResult();
                if (kqlQuery.Length == 2)
                {
                    try
                    {
                        kqlExtractionResult.Id = kqlQuery[0];
                        if (RunExtraction(kqlExtractionResult, Encoding.UTF8.GetString(Convert.FromBase64String(kqlQuery[1]))) == 0)
                        {
                            Console.WriteLine(JsonSerializer.Serialize(kqlExtractionResult));
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("[!] Error: Caught Exception \"{0}\"", e.Message);
                    }
                }
            }
        }

        private static int RunExtraction(KqlExtractionResult kqlExtractionResult, string kql)
        {
            try
            {
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

            return 0;
        }
    }
}
