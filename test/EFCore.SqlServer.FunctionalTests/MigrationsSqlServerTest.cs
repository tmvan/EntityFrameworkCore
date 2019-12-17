// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata.Internal;
using Microsoft.EntityFrameworkCore.SqlServer.Metadata.Internal;
using Microsoft.EntityFrameworkCore.SqlServer.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace Microsoft.EntityFrameworkCore
{
    public class MigrationsSqlServerTest : MigrationsTestBase<MigrationsSqlServerTest.MigrationsSqlServerFixture>
    {
        protected static string EOL => Environment.NewLine;

        public MigrationsSqlServerTest(MigrationsSqlServerFixture fixture, ITestOutputHelper testOutputHelper)
            : base(fixture)
        {
            Fixture.TestSqlLoggerFactory.Clear();
            //Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
        }

        public override async Task CreateIndexOperation_with_filter_where_clause()
        {
            await base.CreateIndexOperation_with_filter_where_clause();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(450) NULL;",
                //
                @"CREATE INDEX [IX_People_Name] ON [People] ([Name]) WHERE [Name] IS NOT NULL;");
        }

        public override async Task CreateIndexOperation_with_filter_where_clause_and_is_unique()
        {
            await base.CreateIndexOperation_with_filter_where_clause_and_is_unique();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(450) NULL;",
                //
                @"CREATE UNIQUE INDEX [IX_People_Name] ON [People] ([Name]) WHERE [Name] IS NOT NULL AND [Name] <> '';");
        }

        public override async Task AddColumnOperation_with_defaultValue_string()
        {
            await base.AddColumnOperation_with_defaultValue_string();

            AssertSql(
                @"ALTER TABLE [People] ADD [Name] nvarchar(max) NOT NULL DEFAULT N'John Doe';");
        }

        public override async Task AddColumnOperation_with_defaultValue_datetime()
        {
            await base.AddColumnOperation_with_defaultValue_datetime();

            AssertSql(
                @"ALTER TABLE [People] ADD [Birthday] datetime2 NOT NULL DEFAULT '2015-04-12T17:05:00.0000000';");
        }

        public override async Task AddColumnOperation_with_defaultValueSql()
        {
            await base.AddColumnOperation_with_defaultValueSql();

            AssertSql(
                @"ALTER TABLE [People] ADD [Birthday] date NULL DEFAULT (CURRENT_TIMESTAMP);");
        }

        public override async Task AddColumnOperation_without_column_type()
        {
            await base.AddColumnOperation_without_column_type();

            AssertSql(
                @"ALTER TABLE [People] ADD [Name] nvarchar(max) NOT NULL DEFAULT N'';");
        }

        public override async Task AddColumnOperation_with_ansi()
        {
            await base.AddColumnOperation_with_ansi();

            AssertSql(
                @"ALTER TABLE [People] ADD [Name] varchar(max) NULL;");
        }

        public override async Task AddColumnOperation_with_fixed_length()
        {
            await base.AddColumnOperation_with_fixed_length();

            AssertSql(
                @"ALTER TABLE [People] ADD [Name] nvarchar(max) NULL;");
        }

        public override async Task AddColumnOperation_with_maxLength()
        {
            await base.AddColumnOperation_with_maxLength();

            AssertSql(
                @"ALTER TABLE [People] ADD [Name] nvarchar(30) NULL;");
        }

        public override async Task AddColumnOperation_with_maxLength_on_derived()
        {
            await base.AddColumnOperation_with_maxLength_on_derived();

            Assert.Empty(Fixture.TestSqlLoggerFactory.SqlStatements);
        }

        public override async Task AddColumnOperation_with_shared_column()
        {
            await base.AddColumnOperation_with_shared_column();

            AssertSql(
                @"ALTER TABLE [Base] ADD [Foo] nvarchar(max) NULL;");
        }

        public override async Task AddColumnOperation_with_computedSql()
        {
            await base.AddColumnOperation_with_computedSql();

            AssertSql(
                @"ALTER TABLE [People] ADD [FullName] AS FirstName + ' ' + LastName;");
        }

        public override async Task AddColumnOperation_with_comment()
        {
            await base.AddColumnOperation_with_comment();

            AssertSql(
                @"ALTER TABLE [People] ADD [FullName] nvarchar(max) NULL;
DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_addextendedproperty 'MS_Description', N'My comment', 'SCHEMA', @defaultSchema, 'TABLE', N'People', 'COLUMN', N'FullName';");
        }

        [ConditionalFact]
        public virtual async Task AddColumnOperation_identity()
        {
            await Test(
                builder => builder.Entity("People").Property<string>("Id"),
                builder => { },
                builder => builder.Entity("People").Property<int>("IdentityColumn").UseIdentityColumn(),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "IdentityColumn");
                    Assert.Equal(ValueGenerated.OnAdd, column.ValueGenerated);
                });

            AssertSql(
                @"ALTER TABLE [People] ADD [IdentityColumn] int NOT NULL IDENTITY;");
        }

        [ConditionalFact]
        public virtual async Task AddColumnOperation_identity_seed_increment()
        {
            await Test(
                builder => builder.Entity("People").Property<string>("Id"),
                builder => { },
                builder => builder.Entity("People").Property<int>("IdentityColumn").UseIdentityColumn(100, 5),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "IdentityColumn");
                    Assert.Equal(ValueGenerated.OnAdd, column.ValueGenerated);
                    // TODO: Do we not reverse-engineer identity facets?
                    // Assert.Equal(100, column[SqlServerAnnotationNames.IdentitySeed]);
                    // Assert.Equal(5, column[SqlServerAnnotationNames.IdentityIncrement]);
                });

            AssertSql(
                @"ALTER TABLE [People] ADD [IdentityColumn] int NOT NULL IDENTITY(100, 5);");
        }

        [ConditionalFact]
        public virtual async Task AddColumnOperation_datetime_with_defaultValue()
        {
            await Test(
                builder => builder.Entity("People").Property<string>("Id"),
                builder => { },
                builder => builder.Entity("People").Property<DateTime>("Birthday")
                    .HasColumnType("datetime")
                    .HasDefaultValue(new DateTime(2019, 1, 1)),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "Birthday");
                    Assert.Contains("2019", column.DefaultValueSql);
                });

            AssertSql(
                @"ALTER TABLE [People] ADD [Birthday] datetime NOT NULL DEFAULT '2019-01-01T00:00:00.000';");
        }

        [ConditionalFact]
        public virtual async Task AddColumnOperation_smalldatetime_with_defaultValue()
        {
            await Test(
                builder => builder.Entity("People").Property<string>("Id"),
                builder => { },
                builder => builder.Entity("People").Property<DateTime>("Birthday")
                    .HasColumnType("smalldatetime")
                    .HasDefaultValue(new DateTime(2019, 1, 1)),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "Birthday");
                    Assert.Contains("2019", column.DefaultValueSql);
                });

            AssertSql(
                @"ALTER TABLE [People] ADD [Birthday] smalldatetime NOT NULL DEFAULT '2019-01-01T00:00:00';");
        }

        [ConditionalFact]
        public virtual async Task AddColumnOperation_with_rowversion()
        {
            await Test(
                builder => builder.Entity("People").Property<int>("Id"),
                builder => { },
                builder => builder.Entity("People").Property<byte[]>("RowVersion").IsRowVersion(),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "RowVersion");
                    Assert.Equal("rowversion", column.StoreType);
                    Assert.True(column.IsRowVersion());
                });

            AssertSql(
                @"ALTER TABLE [People] ADD [RowVersion] rowversion NULL;");
        }

        public override async Task AddForeignKeyOperation()
        {
            await base.AddForeignKeyOperation();

            AssertSql(
                @"CREATE INDEX [IX_Orders_CustomerId] ON [Orders] ([CustomerId]);",
                //
                @"ALTER TABLE [Orders] ADD CONSTRAINT [FK_Orders_Customers_CustomerId] FOREIGN KEY ([CustomerId]) REFERENCES [Customers] ([Id]) ON DELETE CASCADE;");
        }

        public override async Task AddForeignKeyOperation_with_name()
        {
            await base.AddForeignKeyOperation_with_name();

            AssertSql(
                @"CREATE INDEX [IX_Orders_CustomerId] ON [Orders] ([CustomerId]);",
                //
                @"ALTER TABLE [Orders] ADD CONSTRAINT [FK_Foo] FOREIGN KEY ([CustomerId]) REFERENCES [Customers] ([Id]) ON DELETE CASCADE;");
        }

        public override async Task AddPrimaryKeyOperation()
        {
            await base.AddPrimaryKeyOperation();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeField');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [SomeField] nvarchar(450) NOT NULL;",
                //
                @"ALTER TABLE [People] ADD CONSTRAINT [PK_People] PRIMARY KEY ([SomeField]);");
        }

        public override async Task AddPrimaryKeyOperation_composite_with_name()
        {
            await base.AddPrimaryKeyOperation_composite_with_name();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeField2');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [SomeField2] nvarchar(450) NOT NULL;",
                //
                @"DECLARE @var1 sysname;
SELECT @var1 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeField1');
IF @var1 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var1 + '];');
ALTER TABLE [People] ALTER COLUMN [SomeField1] nvarchar(450) NOT NULL;",
                //
                @"ALTER TABLE [People] ADD CONSTRAINT [PK_Foo] PRIMARY KEY ([SomeField1], [SomeField2]);");
        }

        [ConditionalFact]
        public virtual async Task AddPrimaryKeyOperation_nonclustered()
        {
            await Test(
                builder => builder.Entity("People").Property<string>("SomeField"),
                builder => { },
                builder => builder.Entity("People").HasKey("SomeField").IsClustered(false),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var primaryKey = table.PrimaryKey;
                    Assert.False((bool)primaryKey[SqlServerAnnotationNames.Clustered]);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeField');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [SomeField] nvarchar(450) NOT NULL;",
                //
                @"ALTER TABLE [People] ADD CONSTRAINT [PK_People] PRIMARY KEY NONCLUSTERED ([SomeField]);");
        }

        public override async Task AddUniqueConstraintOperation()
        {
            await base.AddUniqueConstraintOperation();

            AssertSql(
                @"ALTER TABLE [People] ADD CONSTRAINT [AK_People_AlternateKeyColumn] UNIQUE ([AlternateKeyColumn]);");
        }

        public override async Task AddUniqueConstraintOperation_composite_with_name()
        {
            await base.AddUniqueConstraintOperation_composite_with_name();

            AssertSql(
                @"ALTER TABLE [People] ADD CONSTRAINT [AK_Foo] UNIQUE ([AlternateKeyColumn1], [AlternateKeyColumn2]);");
        }

        public override async Task CreateCheckConstraintOperation_with_name()
        {
            await base.CreateCheckConstraintOperation_with_name();

            AssertSql(
                @"ALTER TABLE [People] ADD CONSTRAINT [CK_Foo] CHECK ([DriverLicense] > 0);");
        }

        public override async Task AlterColumnOperation_change_type()
        {
            await base.AlterColumnOperation_change_type();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeColumn');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [SomeColumn] bigint NOT NULL;");
        }

        public override async Task AlterColumnOperation_make_required()
        {
            await base.AlterColumnOperation_make_required();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeColumn');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [SomeColumn] nvarchar(max) NOT NULL;");
        }

        [ConditionalFact]
        public override async Task AlterColumnOperation_make_computed()
        {
            await base.AlterColumnOperation_make_computed();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'FullName');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] DROP COLUMN [FullName];
ALTER TABLE [People] ADD [FullName] AS FirstName + ' ' + LastName;");
        }

        [ConditionalFact(Skip = "Column 'FullName' in table 'People' is of a type that is invalid for use as a key column in an index.")]
        public override async Task AlterColumnOperation_change_computed_with_index()
        {
            await base.AlterColumnOperation_change_computed_with_index();

            AssertSql(
                @"");
        }

        [ConditionalFact]
        public override async Task AlterColumnOperation_make_required_with_index()
        {
            await base.AlterColumnOperation_make_required_with_index();

            AssertSql(
                @"DROP INDEX [IX_People_SomeColumn] ON [People];
DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeColumn');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [SomeColumn] nvarchar(450) NOT NULL;
CREATE INDEX [IX_People_SomeColumn] ON [People] ([SomeColumn]);");
        }

        [ConditionalFact]
        public override async Task AlterColumnOperation_make_required_with_composite_index()
        {
            await base.AlterColumnOperation_make_required_with_composite_index();

            AssertSql(
                @"DROP INDEX [IX_People_FirstName_LastName] ON [People];
DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'FirstName');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [FirstName] nvarchar(450) NOT NULL;
CREATE INDEX [IX_People_FirstName_LastName] ON [People] ([FirstName], [LastName]);");
        }

        [ConditionalFact]
        public virtual async Task AlterColumnOperation_make_required_with_index_with_included_properties()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("SomeColumn");
                        e.Property<string>("SomeOtherColumn");
                        e.HasIndex("SomeColumn").IncludeProperties("SomeOtherColumn");
                    }),
                builder => { },
                builder => builder.Entity("People").Property<string>("SomeColumn").IsRequired(),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "SomeColumn");
                    Assert.False(column.IsNullable);
                    var index = Assert.Single(table.Indexes);
                    // TODO: This is a scaffolding bug, #19351
                    Assert.Equal(2, index.Columns.Count);
                    Assert.Contains(table.Columns.Single(c => c.Name == "SomeColumn"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "SomeOtherColumn"), index.Columns);
                });

            AssertSql(
                @"DROP INDEX [IX_People_SomeColumn] ON [People];
DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeColumn');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [SomeColumn] nvarchar(450) NOT NULL;
CREATE INDEX [IX_People_SomeColumn] ON [People] ([SomeColumn]) INCLUDE ([SomeOtherColumn]);");
        }

        [ConditionalFact]
        public override async Task AlterColumnOperation_add_comment()
        {
            await base.AlterColumnOperation_add_comment();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Id');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Id] int NOT NULL;
DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_addextendedproperty 'MS_Description', N'Some comment', 'SCHEMA', @defaultSchema, 'TABLE', N'People', 'COLUMN', N'Id';");
        }

        [ConditionalFact]
        public override async Task AlterColumnOperation_change_comment()
        {
            await base.AlterColumnOperation_change_comment();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Id');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Id] int NOT NULL;
DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_dropextendedproperty 'MS_Description', 'SCHEMA', @defaultSchema, 'TABLE', N'People', 'COLUMN', N'Id';
EXEC sp_addextendedproperty 'MS_Description', N'Some comment2', 'SCHEMA', @defaultSchema, 'TABLE', N'People', 'COLUMN', N'Id';");
        }

        [ConditionalFact]
        public override async Task AlterColumnOperation_remove_comment()
        {
            await base.AlterColumnOperation_remove_comment();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Id');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Id] int NOT NULL;
DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_dropextendedproperty 'MS_Description', 'SCHEMA', @defaultSchema, 'TABLE', N'People', 'COLUMN', N'Id';");
        }

        [ConditionalFact]
        public virtual async Task AlterColumnOperation_memoryOptimized_with_index()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.IsMemoryOptimized();
                        e.Property<int>("Id");
                        e.Property<string>("Name");
                        e.HasIndex("Name");
                    }),
                builder => { },
                builder => builder.Entity("People").Property<string>("Name").HasMaxLength(30),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "Name");
                    Assert.Equal("nvarchar(30)", column.StoreType);
                });

            AssertSql(
                @"ALTER TABLE [People] DROP INDEX [IX_People_Name];
DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(30) NULL;
ALTER TABLE [People] ADD INDEX [IX_People_Name] NONCLUSTERED ([Name]);");
        }

        [ConditionalFact]
        public virtual async Task AlterColumnOperation_with_index_no_narrowing()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("Name");
                        e.HasIndex("Name");
                    }),
                builder => builder.Entity("People").Property<string>("Name").IsRequired(),
                builder => builder.Entity("People").Property<string>("Name").IsRequired(false),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "Name");
                    Assert.True(column.IsNullable);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(450) NULL;");
        }

        [ConditionalFact]
        public virtual async Task AlterColumnOperation_with_index_included_column()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("Name");
                        e.Property<string>("FirstName");
                        e.Property<string>("LastName");
                        e.HasIndex("FirstName", "LastName").IncludeProperties("Name");
                    }),
                builder => { },
                builder => builder.Entity("People").Property<string>("Name").HasMaxLength(30),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var index = Assert.Single(table.Indexes);
                    // TODO: This is a scaffolding bug, #19351
                    Assert.Equal(3, index.Columns.Count);
                    Assert.Contains(table.Columns.Single(c => c.Name == "Name"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "FirstName"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "LastName"), index.Columns);
                });

            AssertSql(
                @"DROP INDEX [IX_People_FirstName_LastName] ON [People];
DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(30) NULL;
CREATE INDEX [IX_People_FirstName_LastName] ON [People] ([FirstName], [LastName]) INCLUDE ([Name]);");
        }

        [ConditionalFact]
        public virtual async Task AlterColumnOperation_type_with_identity()
        {
            await Test(
                builder => builder.Entity("People", e =>
                {
                    e.Property<string>("Id");
                    e.Property<int>("IdentityColumn").UseIdentityColumn();
                }),
                builder => builder.Entity("People", e =>
                {
                    e.Property<string>("Id");
                    e.Property<long>("IdentityColumn").UseIdentityColumn();
                }),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "IdentityColumn");
                    Assert.Equal("bigint", column.StoreType);
                    Assert.Equal(ValueGenerated.OnAdd, column.ValueGenerated);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'IdentityColumn');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [IdentityColumn] bigint NOT NULL;");
        }

        [ConditionalFact(Skip = "Doesn't work")]
        public virtual async Task AlterColumnOperation_remove_identity()
        {
            await Test(
                builder => builder.Entity("People", e =>
                {
                    e.Property<string>("Id");
                    e.Property<int>("SomeColumn").UseIdentityColumn();
                }),
                builder => builder.Entity("People", e =>
                {
                    e.Property<string>("Id");
                    e.Property<int>("SomeColumn");
                }),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "SomeColumn");
                    Assert.Equal(ValueGenerated.Never, column.ValueGenerated);
                });

            AssertSql(
                @"");
        }

        // AlterColumnOperation_remove_identity_legacy

        public override async Task AlterTableOperation_add_comment()
        {
            await base.AlterTableOperation_add_comment();

            AssertSql(
                @"DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_addextendedproperty 'MS_Description', N'Table comment', 'SCHEMA', @defaultSchema, 'TABLE', N'People';");
        }

        public override async Task AlterTableOperation_change_comment()
        {
            await base.AlterTableOperation_change_comment();

            AssertSql(
                @"DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_dropextendedproperty 'MS_Description', 'SCHEMA', @defaultSchema, 'TABLE', N'People';
EXEC sp_addextendedproperty 'MS_Description', N'Table comment2', 'SCHEMA', @defaultSchema, 'TABLE', N'People';");
        }

        public override async Task AlterTableOperation_remove_comment()
        {
            await base.AlterTableOperation_remove_comment();

            AssertSql(
                @"DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_dropextendedproperty 'MS_Description', 'SCHEMA', @defaultSchema, 'TABLE', N'People';");
        }

        [ConditionalFact(Skip = "Need to drop and recreate")]
        public virtual async Task AlterColumnOperation_make_identity()
        {
            await Test(
                builder => builder.Entity("People").Property<int>("SomeColumn"),
                builder => builder.Entity("People").Property<int>("SomeColumn").UseIdentityColumn(),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var column = Assert.Single(table.Columns, c => c.Name == "SomeColumn");
                    Assert.Equal(ValueGenerated.OnAdd, column.ValueGenerated);
                });

            AssertSql(
                @"DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_dropextendedproperty 'MS_Description', 'SCHEMA', @defaultSchema, 'TABLE', N'People';");
        }

        public override async Task AlterSequenceOperation_all_settings()
        {
            await base.AlterSequenceOperation_all_settings();

            AssertSql(
                @"ALTER SEQUENCE [foo] INCREMENT BY 2 MINVALUE -5 MAXVALUE 10 CYCLE;",
                //
                @"ALTER SEQUENCE [foo] RESTART WITH -3;");
        }

        public override async Task AlterSequenceOperation_increment_by()
        {
            await base.AlterSequenceOperation_increment_by();

            AssertSql(
                @"ALTER SEQUENCE [foo] INCREMENT BY 2 NO MINVALUE NO MAXVALUE NO CYCLE;");
        }

        public override async Task RenameTableOperation()
        {
            await base.RenameTableOperation();

            AssertSql(
                @"ALTER TABLE [People] DROP CONSTRAINT [PK_People];",
                //
                @"EXEC sp_rename N'[People]', N'people';",
                //
                @"ALTER TABLE [people] ADD CONSTRAINT [PK_people] PRIMARY KEY ([Id]);");
        }

        public override async Task RenameColumnOperation()
        {
            await base.RenameColumnOperation();

            AssertSql(
                @"EXEC sp_rename N'[People].[SomeColumn]', N'somecolumn', N'COLUMN';");
        }

        public override async Task RenameIndexOperation()
        {
            await base.RenameIndexOperation();

            AssertSql(
                @"EXEC sp_rename N'[People].[Foo]', N'foo', N'INDEX';");
        }

        public override async Task CreateIndexOperation()
        {
            await base.CreateIndexOperation();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'FirstName');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [FirstName] nvarchar(450) NULL;",
                //
                @"CREATE INDEX [IX_People_FirstName] ON [People] ([FirstName]);");
        }

        public override async Task RenameSequenceOperation()
        {
            await base.RenameSequenceOperation();

            AssertSql(
                @"EXEC sp_rename N'[TestSequence]', N'testsequence';");
        }

        public override async Task CreateIndexOperation_unique()
        {
            await base.CreateIndexOperation_unique();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'LastName');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [LastName] nvarchar(450) NULL;",
                //
                @"DECLARE @var1 sysname;
SELECT @var1 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'FirstName');
IF @var1 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var1 + '];');
ALTER TABLE [People] ALTER COLUMN [FirstName] nvarchar(450) NULL;",
                //
                @"CREATE UNIQUE INDEX [IX_People_FirstName_LastName] ON [People] ([FirstName], [LastName]) WHERE [FirstName] IS NOT NULL AND [LastName] IS NOT NULL;");
        }

        public override async Task CreateIndexOperation_with_where_clauses()
        {
            await base.CreateIndexOperation_with_where_clauses();

            AssertSql(
                @"CREATE INDEX [IX_People_Age] ON [People] ([Age]) WHERE [Age] > 18;");
        }

        [ConditionalFact]
        public virtual async Task CreateIndexOperation_clustered()
        {
            await Test(
                builder => builder.Entity("People").Property<string>("FirstName"),
                builder => { },
                builder => builder.Entity("People").HasIndex("FirstName").IsClustered(),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var index = Assert.Single(table.Indexes);
                    Assert.True((bool)index[SqlServerAnnotationNames.Clustered]);
                    Assert.False(index.IsUnique);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'FirstName');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [FirstName] nvarchar(450) NULL;",
                //
                @"CREATE CLUSTERED INDEX [IX_People_FirstName] ON [People] ([FirstName]);");
        }

        [ConditionalFact]
        public virtual async Task CreateIndexOperation_unique_clustered()
        {
            await Test(
                builder => builder.Entity("People").Property<string>("FirstName"),
                builder => { },
                builder => builder.Entity("People").HasIndex("FirstName")
                    .IsUnique()
                    .IsClustered(),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var index = Assert.Single(table.Indexes);
                    Assert.True((bool)index[SqlServerAnnotationNames.Clustered]);
                    Assert.True(index.IsUnique);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'FirstName');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [FirstName] nvarchar(450) NULL;",
                //
                @"CREATE UNIQUE CLUSTERED INDEX [IX_People_FirstName] ON [People] ([FirstName]);");
        }

        [ConditionalFact]
        public virtual async Task CreateIndexOperation_with_include()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("FirstName");
                        e.Property<string>("LastName");
                        e.Property<string>("Name");
                    }),
                builder => { },
                builder => builder.Entity("People").HasIndex("Name")
                    .IncludeProperties("FirstName", "LastName"),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var index = Assert.Single(table.Indexes);
                    // TODO: This is a scaffolding bug, #19351
                    Assert.Equal(3, index.Columns.Count);
                    Assert.Contains(table.Columns.Single(c => c.Name == "Name"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "FirstName"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "LastName"), index.Columns);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(450) NULL;",
                //
                @"CREATE INDEX [IX_People_Name] ON [People] ([Name]) INCLUDE ([FirstName], [LastName]);");
        }

        [ConditionalFact]
        public virtual async Task CreateIndexOperation_with_include_and_filter()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("FirstName");
                        e.Property<string>("LastName");
                        e.Property<string>("Name");
                    }),
                builder => { },
                builder => builder.Entity("People").HasIndex("Name")
                    .IncludeProperties("FirstName", "LastName")
                    .HasFilter("[Name] IS NOT NULL"),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var index = Assert.Single(table.Indexes);
                    Assert.Equal("([Name] IS NOT NULL)", index.Filter);
                    // TODO: This is a scaffolding bug, #19351
                    Assert.Equal(3, index.Columns.Count);
                    Assert.Contains(table.Columns.Single(c => c.Name == "Name"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "FirstName"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "LastName"), index.Columns);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(450) NULL;",
                //
                @"CREATE INDEX [IX_People_Name] ON [People] ([Name]) INCLUDE ([FirstName], [LastName]) WHERE [Name] IS NOT NULL;");
        }

        [ConditionalFact]
        public virtual async Task CreateIndexOperation_unique_with_include()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("FirstName");
                        e.Property<string>("LastName");
                        e.Property<string>("Name").IsRequired();
                    }),
                builder => { },
                builder => builder.Entity("People").HasIndex("Name")
                    .IsUnique()
                    .IncludeProperties("FirstName", "LastName"),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var index = Assert.Single(table.Indexes);
                    Assert.True(index.IsUnique);
                    // TODO: This is a scaffolding bug, #19351
                    Assert.Equal(3, index.Columns.Count);
                    Assert.Contains(table.Columns.Single(c => c.Name == "Name"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "FirstName"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "LastName"), index.Columns);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(450) NOT NULL;",
                //
                @"CREATE UNIQUE INDEX [IX_People_Name] ON [People] ([Name]) INCLUDE ([FirstName], [LastName]);");
        }

        [ConditionalFact]
        public virtual async Task CreateIndexOperation_unique_with_include_and_filter()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("FirstName");
                        e.Property<string>("LastName");
                        e.Property<string>("Name").IsRequired();
                    }),
                builder => { },
                builder => builder.Entity("People").HasIndex("Name")
                    .IsUnique()
                    .IncludeProperties("FirstName", "LastName")
                    .HasFilter("[Name] IS NOT NULL"),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var index = Assert.Single(table.Indexes);
                    Assert.True(index.IsUnique);
                    Assert.Equal("([Name] IS NOT NULL)", index.Filter);
                    // TODO: This is a scaffolding bug, #19351
                    Assert.Equal(3, index.Columns.Count);
                    Assert.Contains(table.Columns.Single(c => c.Name == "Name"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "FirstName"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "LastName"), index.Columns);
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(450) NOT NULL;",
                //
                @"CREATE UNIQUE INDEX [IX_People_Name] ON [People] ([Name]) INCLUDE ([FirstName], [LastName]) WHERE [Name] IS NOT NULL;");
        }

        [ConditionalFact]
        public virtual async Task CreateIndexOperation_unique_with_include_and_filter_online()
        {
            await Test(
                builder => builder.Entity(
                    "People", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<string>("FirstName");
                        e.Property<string>("LastName");
                        e.Property<string>("Name").IsRequired();
                    }),
                builder => { },
                builder => builder.Entity("People").HasIndex("Name")
                    .IsUnique()
                    .IncludeProperties("FirstName", "LastName")
                    .HasFilter("[Name] IS NOT NULL")
                    .IsCreatedOnline(),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var index = Assert.Single(table.Indexes);
                    Assert.True(index.IsUnique);
                    Assert.Equal("([Name] IS NOT NULL)", index.Filter);
                    // TODO: This is a scaffolding bug, #19351
                    Assert.Equal(3, index.Columns.Count);
                    Assert.Contains(table.Columns.Single(c => c.Name == "Name"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "FirstName"), index.Columns);
                    Assert.Contains(table.Columns.Single(c => c.Name == "LastName"), index.Columns);
                    // TODO: Online index not scaffolded?
                });

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Name');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] ALTER COLUMN [Name] nvarchar(450) NOT NULL;",
                //
                @"CREATE UNIQUE INDEX [IX_People_Name] ON [People] ([Name]) INCLUDE ([FirstName], [LastName]) WHERE [Name] IS NOT NULL WITH (ONLINE = ON);");
        }

        public override async Task CreateSequenceOperation_all_settings()
        {
            await base.CreateSequenceOperation_all_settings();

            AssertSql(
                @"IF SCHEMA_ID(N'dbo2') IS NULL EXEC(N'CREATE SCHEMA [dbo2];');",
                //
                @"CREATE SEQUENCE [dbo2].[TestSequence] START WITH 3 INCREMENT BY 2 MINVALUE 2 MAXVALUE 916 CYCLE;");
        }

        public override async Task CreateTableOperation()
        {
            await base.CreateTableOperation();

            AssertSql(
                @"CREATE TABLE [People] (
    [Id] int NOT NULL IDENTITY,
    [Name] nvarchar(max) NULL,
    CONSTRAINT [PK_People] PRIMARY KEY ([Id])
);");
        }

        public override async Task CreateTableOperation_all_settings()
        {
            await base.CreateTableOperation_all_settings();

            AssertSql(
                @"IF SCHEMA_ID(N'dbo2') IS NULL EXEC(N'CREATE SCHEMA [dbo2];');",
                //
                @"CREATE TABLE [dbo2].[People] (
    [CustomId] int NOT NULL IDENTITY,
    [EmployerId] int NOT NULL,
    [SSN] nvarchar(11) NOT NULL,
    CONSTRAINT [PK_People] PRIMARY KEY ([CustomId]),
    CONSTRAINT [AK_People_SSN] UNIQUE ([SSN]),
    CONSTRAINT [CK_SSN] CHECK ([SSN] > 0),
    CONSTRAINT [FK_People_Employers_EmployerId] FOREIGN KEY ([EmployerId]) REFERENCES [Employers] ([Id]) ON DELETE CASCADE
);
EXEC sp_addextendedproperty 'MS_Description', N'Table comment', 'SCHEMA', N'dbo2', 'TABLE', N'People';
EXEC sp_addextendedproperty 'MS_Description', N'Employer ID comment', 'SCHEMA', N'dbo2', 'TABLE', N'People', 'COLUMN', N'EmployerId';",
                //
                @"CREATE INDEX [IX_People_EmployerId] ON [dbo2].[People] ([EmployerId]);");
        }

        public override async Task CreateTableOperation_no_key()
        {
            await base.CreateTableOperation_no_key();

            AssertSql(
                @"CREATE TABLE [Anonymous] (
    [SomeColumn] int NOT NULL
);");
        }

        public override async Task CreateTableOperation_comments()
        {
            await base.CreateTableOperation_comments();

            AssertSql(
                @"CREATE TABLE [People] (
    [Id] int NOT NULL IDENTITY,
    [Name] nvarchar(max) NULL,
    CONSTRAINT [PK_People] PRIMARY KEY ([Id])
);
DECLARE @defaultSchema AS sysname;
SET @defaultSchema = SCHEMA_NAME();
EXEC sp_addextendedproperty 'MS_Description', N'Table comment', 'SCHEMA', @defaultSchema, 'TABLE', N'People';
EXEC sp_addextendedproperty 'MS_Description', N'Column comment', 'SCHEMA', @defaultSchema, 'TABLE', N'People', 'COLUMN', N'Name';");
        }

        public override async Task DropColumnOperation()
        {
            await base.DropColumnOperation();

            AssertSql(
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'SomeColumn');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] DROP COLUMN [SomeColumn];");
        }

        public override async Task DropColumnOperation_primary_key()
        {
            await base.DropColumnOperation_primary_key();

            AssertSql(
                @"ALTER TABLE [People] DROP CONSTRAINT [PK_People];",
                //
                @"DECLARE @var0 sysname;
SELECT @var0 = [d].[name]
FROM [sys].[default_constraints] [d]
INNER JOIN [sys].[columns] [c] ON [d].[parent_column_id] = [c].[column_id] AND [d].[parent_object_id] = [c].[object_id]
WHERE ([d].[parent_object_id] = OBJECT_ID(N'[People]') AND [c].[name] = N'Id');
IF @var0 IS NOT NULL EXEC(N'ALTER TABLE [People] DROP CONSTRAINT [' + @var0 + '];');
ALTER TABLE [People] DROP COLUMN [Id];");
        }

        public override async Task DropForeignKeyOperation()
        {
            await base.DropForeignKeyOperation();

            AssertSql(
                @"ALTER TABLE [Orders] DROP CONSTRAINT [FK_Orders_Customers_CustomerId];",
                //
                @"DROP INDEX [IX_Orders_CustomerId] ON [Orders];");
        }

        public override async Task DropIndexOperation()
        {
            await base.DropIndexOperation();

            AssertSql(
                @"DROP INDEX [IX_People_SomeField] ON [People];");
        }

        // Not supported: To change the IDENTITY property of a column, the column needs to be dropped and recreated.
        public override Task DropPrimaryKeyOperation() => Task.CompletedTask;

        public override async Task DropSequenceOperation()
        {
            await base.DropSequenceOperation();

            AssertSql(
                @"DROP SEQUENCE [TestSequence];");
        }

        public override async Task DropTableOperation()
        {
            await base.DropTableOperation();

            AssertSql(
                @"DROP TABLE [People];");
        }

        public override async Task DropUniqueConstraintOperation()
        {
            await base.DropUniqueConstraintOperation();

            AssertSql(
                @"ALTER TABLE [People] DROP CONSTRAINT [AK_People_AlternateKeyColumn];");
        }

        public override async Task DropCheckConstraintOperation()
        {
            await base.DropCheckConstraintOperation();

            AssertSql(
                @"ALTER TABLE [People] DROP CONSTRAINT [CK_Foo];");
        }

        public override async Task MoveSequenceOperation()
        {
            await base.MoveSequenceOperation();

            AssertSql(
                @"IF SCHEMA_ID(N'TestSequenceSchema') IS NULL EXEC(N'CREATE SCHEMA [TestSequenceSchema];');",
                //
                @"ALTER SCHEMA [TestSequenceSchema] TRANSFER [TestSequence];");
        }

        [ConditionalFact]
        public virtual async Task MoveSequenceOperation_into_default()
        {
            await Test(
                builder => builder.HasSequence<int>("TestSequence", "TestSequenceSchema"),
                builder => builder.HasSequence<int>("TestSequence"),
                model =>
                {
                    var sequence = Assert.Single(model.Sequences);
                    Assert.Equal("dbo", sequence.Schema);
                    Assert.Equal("TestSequence", sequence.Name);
                });

            AssertSql(
                @"DECLARE @defaultSchema sysname = SCHEMA_NAME();
EXEC(N'ALTER SCHEMA [' + @defaultSchema + N'] TRANSFER [TestSequenceSchema].[TestSequence];');");
        }

        public override async Task MoveTableOperation()
        {
            await base.MoveTableOperation();

            AssertSql(
                @"IF SCHEMA_ID(N'TestTableSchema') IS NULL EXEC(N'CREATE SCHEMA [TestTableSchema];');",
                //
                @"ALTER SCHEMA [TestTableSchema] TRANSFER [TestTable];");
        }

        [ConditionalFact]
        public virtual async Task MoveTableOperation_into_default()
        {
            await Test(
                builder => builder.Entity("TestTable")
                    .ToTable("TestTable", "TestTableSchema")
                    .Property<int>("Id"),
                builder => builder.Entity("TestTable")
                    .Property<int>("Id"),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    Assert.Equal("dbo", table.Schema);
                    Assert.Equal("TestTable", table.Name);
                });

            AssertSql(
                @"DECLARE @defaultSchema sysname = SCHEMA_NAME();
EXEC(N'ALTER SCHEMA [' + @defaultSchema + N'] TRANSFER [TestTableSchema].[TestTable];');");
        }

        public override async Task CreateSchemaOperation()
        {
            await base.CreateSchemaOperation();

            AssertSql(
                @"IF SCHEMA_ID(N'SomeOtherSchema') IS NULL EXEC(N'CREATE SCHEMA [SomeOtherSchema];');",
                //
                @"CREATE TABLE [SomeOtherSchema].[People] (
    [Id] int NOT NULL IDENTITY,
    CONSTRAINT [PK_People] PRIMARY KEY ([Id])
);");
        }

        [ConditionalFact]
        public virtual async Task CreateSchemaOperation_dbo()
        {
            await Test(
                builder => { },
                builder => builder.Entity("People")
                    .ToTable("People", "dbo")
                    .Property<int>("Id"),
                model => Assert.Equal("dbo", Assert.Single(model.Tables).Schema));

            AssertSql(
                @"CREATE TABLE [dbo].[People] (
    [Id] int NOT NULL IDENTITY,
    CONSTRAINT [PK_People] PRIMARY KEY ([Id])
);");
        }

        public override async Task InsertDataOperation()
        {
            await base.InsertDataOperation();

            AssertSql(
                @"IF EXISTS (SELECT * FROM [sys].[identity_columns] WHERE [name] IN (N'Id', N'Name') AND [object_id] = OBJECT_ID(N'[Person]'))
    SET IDENTITY_INSERT [Person] ON;
INSERT INTO [Person] ([Id], [Name])
VALUES (1, N'Daenerys Targaryen'),
(2, N'John Snow'),
(3, N'Arya Stark'),
(4, N'Harry Strickland'),
(5, NULL);
IF EXISTS (SELECT * FROM [sys].[identity_columns] WHERE [name] IN (N'Id', N'Name') AND [object_id] = OBJECT_ID(N'[Person]'))
    SET IDENTITY_INSERT [Person] OFF;");
        }

        public override async Task DeleteDataOperation_simple_key()
        {
            await base.DeleteDataOperation_simple_key();

            // TODO remove rowcount
            AssertSql(
                @"DELETE FROM [Person]
WHERE [Id] = 2;
SELECT @@ROWCOUNT;");
        }

        public override async Task DeleteDataOperation_composite_key()
        {
            await base.DeleteDataOperation_composite_key();

            // TODO remove rowcount
            AssertSql(
                @"DELETE FROM [Person]
WHERE [Id] = 2 AND [AnotherId] = 12;
SELECT @@ROWCOUNT;");
        }

        public override async Task UpdateDataOperation_simple_key()
        {
            await base.UpdateDataOperation_simple_key();

            // TODO remove rowcount
            AssertSql(
                @"UPDATE [Person] SET [Name] = N'Another John Snow'
WHERE [Id] = 2;
SELECT @@ROWCOUNT;");
        }

        public override async Task UpdateDataOperation_composite_key()
        {
            await base.UpdateDataOperation_composite_key();

            // TODO remove rowcount
            AssertSql(
                @"UPDATE [Person] SET [Name] = N'Another John Snow'
WHERE [Id] = 2 AND [AnotherId] = 11;
SELECT @@ROWCOUNT;");
        }

        public override async Task UpdateDataOperation_multiple_columns()
        {
            await base.UpdateDataOperation_multiple_columns();

            // TODO remove rowcount
            AssertSql(
                @"UPDATE [Person] SET [Age] = 21, [Name] = N'Another John Snow'
WHERE [Id] = 2;
SELECT @@ROWCOUNT;");
        }

        public class MigrationsSqlServerFixture : MigrationsFixtureBase
        {
            protected override string StoreName { get; } = nameof(MigrationsSqlServerTest);
            protected override ITestStoreFactory TestStoreFactory => SqlServerTestStoreFactory.Instance;
            public override TestHelpers TestHelpers => SqlServerTestHelpers.Instance;

            protected override IServiceCollection AddServices(IServiceCollection serviceCollection)
                => base.AddServices(serviceCollection)
                    .AddScoped<IDatabaseModelFactory, SqlServerDatabaseModelFactory>();
        }
    }
}
