// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public class RelationalParameterBasedQueryPostprocessorFactory : IRelationalParameterBasedQueryPostprocessorFactory
    {
        private readonly RelationalParameterBasedQueryPostprocessorDependencies _dependencies;

        public RelationalParameterBasedQueryPostprocessorFactory(RelationalParameterBasedQueryPostprocessorDependencies dependencies)
        {
            _dependencies = dependencies;
        }

        public RelationalParameterBasedQueryPostprocessor Create(bool useRelationalNulls)
            => new RelationalParameterBasedQueryPostprocessor(_dependencies, useRelationalNulls);
    }
}
