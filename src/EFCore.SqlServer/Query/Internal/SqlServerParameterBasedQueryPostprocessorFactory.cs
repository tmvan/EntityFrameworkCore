// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.SqlServer.Query.Internal
{
    public class SqlServerParameterBasedQueryPostprocessorFactory : IRelationalParameterBasedQueryPostprocessorFactory
    {
        private readonly RelationalParameterBasedQueryPostprocessorDependencies _dependencies;

        public SqlServerParameterBasedQueryPostprocessorFactory(RelationalParameterBasedQueryPostprocessorDependencies dependencies)
        {
            _dependencies = dependencies;
        }

        public RelationalParameterBasedQueryPostprocessor Create(bool useRelationalNulls)
            => new SqlServerParameterBasedQueryPostprocessor(_dependencies, useRelationalNulls);
    }
}
