using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Z.BulkOperations;

namespace BulkTest {
    internal class Program {
        static void Main(string[] args) {
            Console.WriteLine("Hello, World!");

            // Both method do the same 
            // 1. Add a single record
            // 2. Update 1 record, Add 1 record

            // Test bulk insert with ColumnPrimaryKeyNames using context and list
            //Test1(); // works!

            // Test bulk insert with ColumnPrimaryKeyNames using BulkOperation and IDataReader
            Test2(); // fails with: 'Cannot insert the value NULL into column 'Id', table 'BulkTest.dbo.Items'; column does not allow nulls. UPDATE fails.

        }

        private static  void Test2() {
            using(var cnn = new SqlConnection(TestContext.ConnectionString)) {
                cnn.Open();
                using(var bulk = new BulkOperation(cnn)) {
                    var org_reader = new TestReader(new List<Item> {
                        new Item { Id = Guid.NewGuid(), Name = "test3", Count = 0 }
                    });
                    bulk.DestinationTableName = "Items";
                    bulk.ColumnPrimaryKeyNames = new List<string> { "Name" };
                    bulk.BulkMerge(org_reader);
                }

                using(var bulk = new BulkOperation(cnn)) {
                    var org_reader = new TestReader(new List<Item> {
                        new Item { Id = Guid.NewGuid(), Name = "test3", Count = 1 },
                        new Item { Id = Guid.NewGuid(), Name = "test4", Count = 0 },
                    });
                    bulk.DestinationTableName = "Items";
                    bulk.ColumnPrimaryKeyNames = new List<string> { "Name"  };
                    bulk.BulkMerge(org_reader);
                }
            }
        }

        private static void Test1() {
            using(var ctx = new TestContext()) {

                var org_items = new List<Item> {
                    new Item { Id = Guid.NewGuid(), Name = "test1", Count = 0  }
                };

                ctx.BulkMerge(org_items, options => {
                    options.ColumnPrimaryKeyNames = new List<string> { "Name" };
                });

                var upd_items = new List<Item> {
                    new Item { Id = Guid.NewGuid(), Name = "test1", Count = 1  },
                    new Item { Id = Guid.NewGuid(), Name = "test2", Count = 0  }
                };

                ctx.BulkMerge(upd_items, options => {
                    options.ColumnPrimaryKeyExpression = c => new { c.Name };
                });
            }
        }

        public class TestReader : IDataReader {

            public TestReader(List<Item> items) {
                _items = items;
            }

            private List<Item> _items;

            private int pointer = -1;


            public object this[int i] => throw new NotImplementedException();

            public object this[string name] => throw new NotImplementedException();

            public int Depth => throw new NotImplementedException();

            public bool IsClosed => throw new NotImplementedException();

            public int RecordsAffected => -1;

            public int FieldCount => 3;

            public void Close() {
                throw new NotImplementedException();
            }

            public void Dispose() {
                throw new NotImplementedException();
            }

            public bool GetBoolean(int i) {
                throw new NotImplementedException();
            }

            public byte GetByte(int i) {
                throw new NotImplementedException();
            }

            public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) {
                throw new NotImplementedException();
            }

            public char GetChar(int i) {
                throw new NotImplementedException();
            }

            public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) {
                throw new NotImplementedException();
            }

            public IDataReader GetData(int i) {
                throw new NotImplementedException();
            }

            public string GetDataTypeName(int i) {
                throw new NotImplementedException();
            }

            public DateTime GetDateTime(int i) {
                throw new NotImplementedException();
            }

            public decimal GetDecimal(int i) {
                throw new NotImplementedException();
            }

            public double GetDouble(int i) {
                throw new NotImplementedException();
            }

            [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
            public Type GetFieldType(int i) {
                throw new NotImplementedException();
            }

            public float GetFloat(int i) {
                throw new NotImplementedException();
            }

            public Guid GetGuid(int i) {
                throw new NotImplementedException();
            }

            public short GetInt16(int i) {
                throw new NotImplementedException();
            }

            public int GetInt32(int i) {
                throw new NotImplementedException();
            }

            public long GetInt64(int i) {
                throw new NotImplementedException();
            }

            public string GetName(int i) {
                switch(i) {
                    case 0: return "Id";
                    case 1: return "Name";
                    case 2: return "Count";
                }
                throw new NotImplementedException();
            }

            public int GetOrdinal(string name) {
                switch(name) {
                    case "Id": return 0;
                    case "Name": return 1;
                    case "Count": return 2;
                }
                throw new NotImplementedException();
            }

            public DataTable? GetSchemaTable() {
                throw new NotImplementedException();
            }

            public string GetString(int i) {
                throw new NotImplementedException();
            }

            public object GetValue(int i) {
                var record = _items[pointer];
                switch(i) {
                    case 0: return record.Id;
                    case 1: return record.Name;
                    case 2: return record.Count;
                }
                throw new NotImplementedException();
            }

            public int GetValues(object[] values) {
                throw new NotImplementedException();
            }

            public bool IsDBNull(int i) {
                throw new NotImplementedException();
            }

            public bool NextResult() {
                throw new NotImplementedException();
            }

            public bool Read() {
                pointer++;
                return pointer < _items.Count;
            }
        }
    }
    public class TestContext : DbContext {

        public static string ConnectionString = "Server=(localdb)\\mssqllocaldb;Database=BulkTest;Trusted_Connection=True;MultipleActiveResultSets=true";

        public DbSet<Item> Items { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder) {
            base.OnModelCreating(modelBuilder);
            modelBuilder.ApplyConfigurationsFromAssembly(GetType().Assembly);
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) {
            optionsBuilder.UseSqlServer(ConnectionString);
        }
    }

    public class Item {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public int Count { get; set; }
    }
}