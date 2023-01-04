using SqlBenchmarker;
using System.Data;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

if (args.Length != 2)
{
    Console.Error.WriteLine("Usage: SqlBenchmarker [Microsoft|System] <size in MB>");
    return -1;
}

IDbConnection connection;

switch (args[0])
{
    case "Microsoft":
    {
            var csb = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder();
            csb.DataSource = ".";
            csb.ApplicationName = "SQL XML Demo";
            csb.IntegratedSecurity = true;
            csb.Encrypt = false;
            connection = new Microsoft.Data.SqlClient.SqlConnection(csb.ToString());
            break;
    }

    case "System":
        {
            var csb = new System.Data.SqlClient.SqlConnectionStringBuilder();
            csb.DataSource = ".";
            csb.ApplicationName = "SQL XML Demo";
            csb.IntegratedSecurity = true;
            csb.Encrypt = false;
            connection = new System.Data.SqlClient.SqlConnection(csb.ToString());
            break;
    }

    default:
        Console.Error.WriteLine("Error: Provider must be either 'Microsoft' or 'System'.");
        return -1;
}

if (!int.TryParse(args[1], out var sizeInMB) || sizeInMB <= 0)
{
    Console.Error.WriteLine("Error: Size must be a valid positive integer");
    return -1;
}

// Use the same unique ID every time for exact reproducibility
var primaryKey = new Guid("4506E355-B727-4112-82A6-52E0258BAB0D");


using (connection)
{
    connection.Open();
    Console.WriteLine("Connected to SQL Server.");

    connection.ExecuteNonQuery("CREATE DATABASE [SqlXmlDemo];");
    connection.ExecuteNonQuery("USE [SqlXmlDemo];");

    Console.WriteLine("Created SqlXmlDemo database.");

    connection.ExecuteNonQuery(@"CREATE TABLE [Foo]
                (
                    [PK]        UNIQUEIDENTIFIER    NOT NULL    PRIMARY KEY,
                    [XmlData]   xml                 NOT NULL
                );");

    var stopwatch = new Stopwatch();
    stopwatch.Start();
    var dummyXml = CreateDummyXml(sizeInMB * 1024 * 1024); // Megabytes (technically Mibibytes)
    stopwatch.Stop();
    Console.WriteLine($"Time taken to create {sizeInMB}MB of xml: {stopwatch.Elapsed}");
    stopwatch.Reset();

    // Insert 10MB of XML into our table.
    using (var command = connection.CreateCommand())
    {
        command.CommandText = "INSERT INTO [Foo] ([PK], [XmlData]) VALUES (@PK, @XmlData);";
        command.AddParameter("PK", DbType.Guid).Value = primaryKey;
        command.AddParameter("XmlData", DbType.Xml).Value = dummyXml;

        stopwatch.Start();
        command.ExecuteNonQuery();
        stopwatch.Stop();
        Console.WriteLine($"Time taken to insert {sizeInMB}MB of xml: {stopwatch.Elapsed}");
        stopwatch.Reset();
    }

    // Actual reading bit
    stopwatch.Start();
    using (var reader = new SqlXmlColumnReader(
        connection,
        "Foo",
        "PK",
        "XmlData",
        primaryKey))
    {
        // Gets immediately garbage-collected, but we've done the work so we have
        // our benchmark time.
        var text = reader.ReadToEnd();
    }
    stopwatch.Stop();
    Console.WriteLine($"Time taken to read {sizeInMB}MB of xml: {stopwatch.Elapsed}");
    // End actual reading bit

    Console.WriteLine("Done!");
    connection.ExecuteNonQuery("USE [master];");
    connection.ExecuteNonQuery("DROP DATABASE [SqlXmlDemo];");
    Console.WriteLine("Dropped SqlXmlDemo database.");

    return 0;
}

static string CreateDummyXml(int targetSize)
{
    // Base64 uses 4 characters - thus 4 bytes of output - to represent 3 bytes of input.
    // Our wrapper tag "<Foo></Foo>" adds 11 bytes;
    var builder = new StringBuilder(targetSize);
    builder.Append("<Foo>");

    var targetBase64Length = targetSize - 11;
    if (targetBase64Length > 0)
    {
        var randomDataSize = targetSize * 3 / 4;
        if (randomDataSize > 0)
        {
            var randomData = new byte[randomDataSize];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetNonZeroBytes(randomData);
            }

            builder.Append(Convert.ToBase64String(randomData));
        }
    }

    builder.Append("</Foo>");
    return builder.ToString();
}