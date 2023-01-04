using System.Data;
using System.Globalization;

namespace SqlBenchmarker
{
    class SqlXmlColumnReader : TextReader
    {
        public SqlXmlColumnReader(IDbConnection connection, string table, string primaryKeyColumn, string xmlDataColumn, object primaryKeyValue)
        {
            lazyReader = new Lazy<IDataReader>(() =>
            {
                // If you ever use this in an actual application, ensure that the parameters are
                // validated to ensure no SQL injection.
                var commandText = string.Format(
                    CultureInfo.InvariantCulture,
                    "SELECT [{0}] FROM [{1}] WHERE [{2}] = @pkvalue;",
                    xmlDataColumn,
                    table,
                    primaryKeyColumn);
                command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;
                command.AddParameter("pkvalue", DbType.Guid).Value = primaryKeyValue;

                // SequentialAccess: "Provides a way for the DataReader to handle rows that
                // contain columns with large binary values. Rather than loading the entire row,
                // SequentialAccess enables the DataReader to load data as a stream.
                // You can then use the GetBytes or GetChars method to specify a byte
                // location to start the read operation, and a limited buffer size for the data
                // being returned."
                var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);

                if (!reader.Read())
                {
                    throw new InvalidOperationException("No record exists with the given primary key value.");
                }

                return reader;
            });
        }

        Lazy<IDataReader>? lazyReader;
        IDbCommand? command;
        long position;

        public override int Peek()
        {
            return -1;
        }

        public override int Read()
        {
            var buffer = new char[1];
            var numCharsRead = lazyReader!.Value.GetChars(0, position, buffer, 0, 1);
            if (numCharsRead == 0)
            {
                return -1;
            }

            position += numCharsRead;

            return buffer[0];
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (lazyReader != null)
                {
                    if (lazyReader.IsValueCreated)
                    {
                        lazyReader.Value.Dispose();
                        lazyReader = null;
                    }
                }

                if (command != null)
                {
                    command.Dispose();
                    command = null;
                }
            }

            base.Dispose(disposing);
        }
    }
}
