using System.Data.SqlClient;

namespace SqlBenchmarker
{
    static partial class SqlExtensions
    {
        public static int ExecuteNonQuery(this SqlConnection connection, string commandText)
        {
            using var command = new SqlCommand(commandText);
            command.Connection = connection;
            return command.ExecuteNonQuery();
        }

        public static async Task<int> ExecuteNonQueryAsync(this SqlConnection connection, string commandText)
        {
            using var command = new SqlCommand(commandText);
            command.Connection = connection;
            return await command.ExecuteNonQueryAsync();
        }
    }
}
