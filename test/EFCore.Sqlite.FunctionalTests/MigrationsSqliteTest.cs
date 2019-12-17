// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Sqlite.Internal;
using Microsoft.EntityFrameworkCore.Sqlite.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace Microsoft.EntityFrameworkCore
{
    public class MigrationsSqliteTest : MigrationsTestBase<MigrationsSqliteTest.MigrationsSqliteFixture>
    {
        public MigrationsSqliteTest(MigrationsSqliteFixture fixture, ITestOutputHelper testOutputHelper)
            : base(fixture)
        {
            Fixture.TestSqlLoggerFactory.Clear();
            //Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
        }

        public override Task CreateIndexOperation_with_filter_where_clause()
            => Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("Name");
                    }),
                builder => { },
                builder => builder.Entity("People").HasIndex("Name").HasFilter($"{DelimitIdentifier("Name")} IS NOT NULL"),
                // Reverse engineering of index filters isn't supported in SQLite
                model => Assert.Null(model.Tables.Single().Indexes.Single().Filter));

        public override Task CreateIndexOperation_with_filter_where_clause_and_is_unique()
            => Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("Name");
                    }),
                builder => { },
                builder => builder.Entity("People").HasIndex("Name").IsUnique()
                    .HasFilter($"{DelimitIdentifier("Name")} IS NOT NULL AND {DelimitIdentifier("Name")} <> ''"),
                // Reverse engineering of index filters isn't supported in SQLite
                model => Assert.Null(model.Tables.Single().Indexes.Single().Filter));

        public override Task CreateIndexOperation_with_where_clauses()
            => Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<int>("Age");
                    }),
                builder => { },
                builder => builder.Entity("People").HasIndex("Age").HasFilter($"{DelimitIdentifier("Age")} > 18"),
                // Reverse engineering of index filters isn't supported in SQLite
                model => Assert.Null(model.Tables.Single().Indexes.Single().Filter));

        public override async Task RenameColumnOperation()
        {
            await base.RenameColumnOperation();

            AssertSql(
                @"ALTER TABLE ""People"" RENAME COLUMN ""SomeColumn"" TO ""somecolumn"";");
        }

        public override async Task RenameIndexOperation()
        {
            await base.RenameIndexOperation();

            AssertSql(
                @"DROP INDEX ""Foo"";
CREATE INDEX ""foo"" ON ""People"" (""FirstName"");");
        }

        public override async Task AddColumnOperation_with_defaultValue_datetime()
        {
            await base.AddColumnOperation_with_defaultValue_datetime();

            AssertSql(
                @"ALTER TABLE ""People"" ADD ""Birthday"" TEXT NOT NULL DEFAULT '2015-04-12 17:05:00';");
        }

        public override async Task AddColumnOperation_with_maxLength()
        {
            await base.AddColumnOperation_with_maxLength();

            // See issue #3698
            AssertSql(
                @"ALTER TABLE ""People"" ADD ""Name"" TEXT NULL;");
        }

        public override Task AddColumnOperation_with_computedSql()
            => AssertNotSupportedAsync(base.AddColumnOperation_with_computedSql, SqliteStrings.ComputedColumnsNotSupported);

        public override async Task AddColumnOperation_with_defaultValueSql()
        {
            var ex = await Assert.ThrowsAsync<SqliteException>(base.AddColumnOperation_with_defaultValueSql);
            Assert.Contains("Cannot add a column with non-constant default", ex.Message);
        }

        // In Sqlite, comments are only generated when creating a table
        public override async Task AddColumnOperation_with_comment()
        {
            await Test(
                builder => builder.Entity("People").Property<int>("Id"),
                builder => { },
                builder => builder.Entity("People").Property<string>("FullName").HasComment("My comment"),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "FullName");
                    Assert.Null(column.Comment);
                });

            AssertSql(
                @"ALTER TABLE ""People"" ADD ""FullName"" TEXT NULL;");
        }

        public override Task AlterColumnOperation_make_required()
            => AssertNotSupportedAsync(base.AlterColumnOperation_make_required, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task AlterColumnOperation_make_computed()
            => AssertNotSupportedAsync(base.AlterColumnOperation_make_computed, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task AlterColumnOperation_change_computed_with_index()
            => AssertNotSupportedAsync(base.AlterColumnOperation_change_computed_with_index, SqliteStrings.ComputedColumnsNotSupported);

        public override Task AlterColumnOperation_make_required_with_composite_index()
            => AssertNotSupportedAsync(base.AlterColumnOperation_make_required_with_composite_index, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task AlterColumnOperation_make_required_with_index()
            => AssertNotSupportedAsync(base.AlterColumnOperation_make_required_with_index, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task AlterColumnOperation_add_comment()
            => AssertNotSupportedAsync(base.AlterColumnOperation_add_comment, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task AlterColumnOperation_change_comment()
            => AssertNotSupportedAsync(base.AlterColumnOperation_change_comment, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task AlterColumnOperation_remove_comment()
            => AssertNotSupportedAsync(base.AlterColumnOperation_remove_comment, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task DropColumnOperation()
            => AssertNotSupportedAsync(base.DropColumnOperation, SqliteStrings.InvalidMigrationOperation("DropColumnOperation"));

        public override Task DropColumnOperation_primary_key()
            => AssertNotSupportedAsync(base.DropColumnOperation_primary_key, SqliteStrings.InvalidMigrationOperation("DropPrimaryKeyOperation"));

        public override Task AddForeignKeyOperation()
            => AssertNotSupportedAsync(base.AddForeignKeyOperation, SqliteStrings.InvalidMigrationOperation("AddForeignKeyOperation"));

        public override Task DropForeignKeyOperation()
            => AssertNotSupportedAsync(base.DropForeignKeyOperation, SqliteStrings.InvalidMigrationOperation("DropForeignKeyOperation"));

        public override Task AddForeignKeyOperation_with_name()
            => AssertNotSupportedAsync(base.AddForeignKeyOperation_with_name, SqliteStrings.InvalidMigrationOperation("AddForeignKeyOperation"));

        public override Task AddPrimaryKeyOperation()
            => AssertNotSupportedAsync(base.AddPrimaryKeyOperation, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task AddPrimaryKeyOperation_composite_with_name()
            => AssertNotSupportedAsync(base.AddPrimaryKeyOperation_composite_with_name, SqliteStrings.InvalidMigrationOperation("AlterColumnOperation"));

        public override Task DropPrimaryKeyOperation()
            => AssertNotSupportedAsync(base.DropPrimaryKeyOperation, SqliteStrings.InvalidMigrationOperation("DropPrimaryKeyOperation"));

        public override Task AddUniqueConstraintOperation()
            => AssertNotSupportedAsync(base.AddUniqueConstraintOperation, SqliteStrings.InvalidMigrationOperation("AddUniqueConstraintOperation"));

        public override Task AddUniqueConstraintOperation_composite_with_name()
            => AssertNotSupportedAsync(base.AddUniqueConstraintOperation_composite_with_name, SqliteStrings.InvalidMigrationOperation("AddUniqueConstraintOperation"));

        public override Task DropUniqueConstraintOperation()
            => AssertNotSupportedAsync(base.DropUniqueConstraintOperation, SqliteStrings.InvalidMigrationOperation("DropUniqueConstraintOperation"));

        public override Task CreateSequenceOperation()
            => AssertNotSupportedAsync(base.CreateSequenceOperation, SqliteStrings.SequencesNotSupported);

        public override Task CreateSequenceOperation_all_settings()
            => AssertNotSupportedAsync(base.CreateSequenceOperation, SqliteStrings.SequencesNotSupported);

        public override Task AlterSequenceOperation_all_settings()
            => AssertNotSupportedAsync(base.CreateSequenceOperation, SqliteStrings.SequencesNotSupported);

        public override Task AlterSequenceOperation_increment_by()
            => AssertNotSupportedAsync(base.CreateSequenceOperation, SqliteStrings.SequencesNotSupported);

        public override Task RenameSequenceOperation()
            => AssertNotSupportedAsync(base.CreateSequenceOperation, SqliteStrings.SequencesNotSupported);

        public override Task MoveSequenceOperation()
            => AssertNotSupportedAsync(base.CreateSequenceOperation, SqliteStrings.SequencesNotSupported);

        public override Task DropSequenceOperation()
            => AssertNotSupportedAsync(base.CreateSequenceOperation, SqliteStrings.SequencesNotSupported);

        public override async Task CreateTableOperation()
        {
            await base.CreateTableOperation();

            AssertSql(
                @"CREATE TABLE ""People"" (
    ""Id"" INTEGER NOT NULL CONSTRAINT ""PK_People"" PRIMARY KEY AUTOINCREMENT,
    ""Name"" TEXT NULL
);");
        }

        // SQLite does not support schemas, check constraints, etc.
        public override Task CreateTableOperation_all_settings() => Task.CompletedTask;

        public async Task CreateTableOperation_old_autoincrement_annotation()
        {
            await Test(
                builder => { },
                builder => builder.Entity("People", e =>
                {
                    e.Property<int>("Id");
                    e.Property<string>("Name").HasComment("Column comment");
                    e.HasComment("Table comment");
                }),
                model =>
                {
                    // Reverse-engineering of comments isn't supported in Sqlite
                    var table = Assert.Single(model.Tables);
                    Assert.Null(table.Comment);
                    var column = Assert.Single(table.Columns, c => c.Name == "Name");
                    Assert.Null(column.Comment);
                });

            AssertSql(
                @"CREATE TABLE ""People"" (
    -- Table comment

    ""Id"" INTEGER NOT NULL CONSTRAINT ""PK_People"" PRIMARY KEY AUTOINCREMENT,

    -- Column comment
    ""Name"" TEXT NULL
);");
        }

        public override async Task CreateTableOperation_comments()
        {
            await Test(
                builder => { },
                builder => builder.Entity("People", e =>
                {
                    e.Property<int>("Id");
                    e.Property<string>("Name").HasComment("Column comment");
                    e.HasComment("Table comment");
                }),
                model =>
                {
                    // Reverse-engineering of comments isn't supported in Sqlite
                    var table = Assert.Single(model.Tables);
                    Assert.Null(table.Comment);
                    var column = Assert.Single(table.Columns, c => c.Name == "Name");
                    Assert.Null(column.Comment);
                });

            AssertSql(
                @"CREATE TABLE ""People"" (
    -- Table comment

    ""Id"" INTEGER NOT NULL CONSTRAINT ""PK_People"" PRIMARY KEY AUTOINCREMENT,

    -- Column comment
    ""Name"" TEXT NULL
);");
        }

        [ConditionalFact]
        public async Task CreateTableOperation_has_multiline_table_comment()
        {
            await Test(
                builder => { },
                builder => builder.Entity("People", e =>
                {
                    e.Property<int>("Id");
                    e.Property<string>("Name");
                    e.HasComment(@"This is a multi-line
table comment.
More information can
be found in the docs.");
                }),
                model =>
                {
                    // Reverse-engineering of comments isn't supported in Sqlite
                    var table = Assert.Single(model.Tables);
                    Assert.Null(table.Comment);
                    var column = Assert.Single(table.Columns, c => c.Name == "Name");
                    Assert.Null(column.Comment);
                });

            AssertSql(
                @"CREATE TABLE ""People"" (
    -- This is a multi-line
    -- table comment.
    -- More information can
    -- be found in the docs.

    ""Id"" INTEGER NOT NULL CONSTRAINT ""PK_People"" PRIMARY KEY AUTOINCREMENT,
    ""Name"" TEXT NULL
);");
        }

        [ConditionalFact]
        public async Task CreateTableOperation_has_multiline_column_comment()
        {
            await Test(
                builder => { },
                builder => builder.Entity("People", e =>
                {
                    e.Property<int>("Id");
                    e.Property<string>("Name").HasComment(@"This is a multi-line
column comment.
More information can
be found in the docs.");
                }),
                model =>
                {
                    // Reverse-engineering of comments isn't supported in Sqlite
                    var table = Assert.Single(model.Tables);
                    Assert.Null(table.Comment);
                    var column = Assert.Single(table.Columns, c => c.Name == "Name");
                    Assert.Null(column.Comment);
                });

            AssertSql(
                @"CREATE TABLE ""People"" (
    ""Id"" INTEGER NOT NULL CONSTRAINT ""PK_People"" PRIMARY KEY AUTOINCREMENT,

    -- This is a multi-line
    -- column comment.
    -- More information can
    -- be found in the docs.
    ""Name"" TEXT NULL
);");
        }

        // In Sqlite, comments are only generated when creating a table
        public override async Task AlterTableOperation_add_comment()
        {
            await Test(
                builder => builder.Entity("People").Property<int>("Id"),
                builder => builder.Entity("People").HasComment("Table comment").Property<int>("Id"),
                model => Assert.Null(Assert.Single(model.Tables).Comment));

            AssertSql();
        }

        // In Sqlite, comments are only generated when creating a table
        public override async Task AlterTableOperation_change_comment()
        {
            await Test(
                builder => builder.Entity("People").HasComment("Table comment1").Property<int>("Id"),
                builder => builder.Entity("People").HasComment("Table comment2").Property<int>("Id"),
                model => Assert.Null(Assert.Single(model.Tables).Comment));

            AssertSql();
        }

        public override Task RenameTableOperation()
            => AssertNotSupportedAsync(base.RenameTableOperation, SqliteStrings.InvalidMigrationOperation("DropPrimaryKeyOperation"));

        // SQLite does not support schemas.
        public override Task MoveTableOperation()
            => Test(
                builder => builder.Entity("TestTable").Property<int>("Id"),
                builder => { },
                builder => builder.Entity("TestTable").ToTable("TestTable", "TestTableSchema"),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    Assert.Null(table.Schema);
                    Assert.Equal("TestTable", table.Name);
                });

        public override Task CreateCheckConstraintOperation_with_name()
            => AssertNotSupportedAsync(base.CreateCheckConstraintOperation_with_name, SqliteStrings.InvalidMigrationOperation("CreateCheckConstraintOperation"));

        public override Task DropCheckConstraintOperation()
            => AssertNotSupportedAsync(base.DropCheckConstraintOperation, SqliteStrings.InvalidMigrationOperation("DropCheckConstraintOperation"));

        // SQLite does not support schemas
        public override Task CreateSchemaOperation()
            => Test(
                builder => { },
                builder => builder.Entity("People")
                    .ToTable("People", "SomeOtherSchema")
                    .Property<int>("Id"),
                model => Assert.Null(Assert.Single(model.Tables).Schema));

        protected virtual async Task AssertNotSupportedAsync(Func<Task> action, string? message = null)
        {
            var ex = await Assert.ThrowsAsync<NotSupportedException>(action);
            if (message != null)
            {
                Assert.Equal(message, ex.Message);
            }
        }

        public class MigrationsSqliteFixture : MigrationsFixtureBase
        {
            protected override string StoreName { get; } = nameof(MigrationsSqliteTest);
            protected override ITestStoreFactory TestStoreFactory => SqliteTestStoreFactory.Instance;
            public override TestHelpers TestHelpers => SqliteTestHelpers.Instance;

            protected override IServiceCollection AddServices(IServiceCollection serviceCollection)
                => base.AddServices(serviceCollection)
                    .AddScoped<IDatabaseModelFactory, SqliteDatabaseModelFactory>();
        }
    }
}
