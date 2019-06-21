// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.SqlServer.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Microsoft.EntityFrameworkCore
{
    public class InterceptionSqlServerTest
        : InterceptionTestBase<SqlServerDbContextOptionsBuilder, SqlServerOptionsExtension>,
            IClassFixture<InterceptionSqlServerTest.InterceptionSqlServerFixture>
    {
        private const string DatabaseName = "Interception";

        public InterceptionSqlServerTest(InterceptionSqlServerFixture fixture)
            : base(fixture)
        {
        }

        public override async Task<string> Intercept_query_passively(bool async, bool inject, bool enableLogging)
        {
            AssertSql(
                @"SELECT [s].[Id], [s].[Type] FROM [Singularity] AS [s]",
                await base.Intercept_query_passively(async, inject, enableLogging));

            return null;
        }

        public override async Task<List<string>> Use_Database_Log_for_query(
            bool async, LogLevel level, bool setInConstructor, bool setInOnConfiguring)
        {
            var log = await base.Use_Database_Log_for_query(async, level, setInConstructor, setInOnConfiguring);
            var position = 0;

            if (level != LogLevel.Warning)
            {
                AssertLog(
                    @"Executing DbCommand [Parameters=[], CommandType='Text', CommandTimeout='30']
SELECT [s].[Id], [s].[Type]
FROM [Singularity] AS [s]", log[position++]);
            }

            if (level == LogLevel.Debug)
            {
                AssertLog(
                    @"Executed DbCommand (3ms) [Parameters=[], CommandType='Text', CommandTimeout='30']
SELECT [s].[Id], [s].[Type]
FROM [Singularity] AS [s]", log[position]);
            }

            return null;
        }

        public override async Task<string> Intercept_query_to_mutate_command(bool async, bool inject, bool enableLogging)
        {
            AssertSql(
                @"SELECT [s].[Id], [s].[Type] FROM [Brane] AS [s]",
                await base.Intercept_query_to_mutate_command(async, inject, enableLogging));

            return null;
        }

        public override async Task<string> Intercept_query_to_replace_execution(bool async, bool inject, bool enableLogging)
        {
            AssertSql(
                @"SELECT [s].[Id], [s].[Type] FROM [Singularity] AS [s]",
                await base.Intercept_query_to_replace_execution(async, inject, enableLogging));

            return null;
        }

        public class InterceptionSqlServerFixture : InterceptionFixtureBase
        {
            protected override string StoreName => DatabaseName;

            protected override ITestStoreFactory TestStoreFactory => SqlServerTestStoreFactory.Instance;

            public override DbContextOptions AddRelationalOptions(
                Action<RelationalDbContextOptionsBuilder<SqlServerDbContextOptionsBuilder, SqlServerOptionsExtension>> relationalBuilder,
                Type[] injectedInterceptorTypes)
                => AddOptions(
                        ((SqlServerTestStore)TestStore)
                        .AddProviderOptions(
                            new DbContextOptionsBuilder()
                                .UseInternalServiceProvider(
                                    InjectInterceptors(
                                            new ServiceCollection()
                                                .AddEntityFrameworkSqlServer(),
                                            injectedInterceptorTypes)
                                        .BuildServiceProvider()),
                            relationalBuilder))
                    .EnableDetailedErrors()
                    .Options;
        }
    }
}
