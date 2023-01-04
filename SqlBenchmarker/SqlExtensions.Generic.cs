using System.Data;

namespace SqlBenchmarker
{
    static partial class SqlExtensions
    {
        public static int ExecuteNonQuery(this IDbConnection connection, string commandText)
        {
            using var command = connection.CreateCommand();
            command.CommandText = commandText;
            return command.ExecuteNonQuery();
        }

        public static IDbDataParameter AddParameter(this IDbCommand command, string name, DbType type)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = type;
            command.Parameters.Add(parameter);
            return parameter;
        }
    }
}
