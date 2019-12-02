// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query
{
    public class RelationalParameterBasedQueryTranslationPostprocessor
    {
        public RelationalParameterBasedQueryTranslationPostprocessor(
            [NotNull] RelationalParameterBasedQueryTranslationPostprocessorDependencies dependencies,
            bool useRelationalNulls)
        {
            Check.NotNull(dependencies, nameof(dependencies));

            Dependencies = dependencies;
            UseRelationalNulls = useRelationalNulls;
        }

        protected virtual RelationalParameterBasedQueryTranslationPostprocessorDependencies Dependencies { get; }

        protected virtual bool UseRelationalNulls { get; }

        public virtual (SelectExpression selectExpression, bool canCache) Optimize(
            [NotNull] SelectExpression selectExpression,
            [NotNull] IReadOnlyDictionary<string, object> parametersValues)
        {
            Check.NotNull(selectExpression, nameof(selectExpression));
            Check.NotNull(parametersValues, nameof(parametersValues));

            var canCache = true;
            var (nullabilityHandled, nullabilityCanCache) = new NullabilityHandlingExpressionVisitor(
                UseRelationalNulls,
                Dependencies.SqlExpressionFactory,
                parametersValues).HandleNullability(selectExpression);

            canCache &= nullabilityCanCache;

            var nullParametersOptimized = new SqlExpressionOptimizingExpressionVisitor(
                Dependencies.SqlExpressionFactory, UseRelationalNulls, parametersValues).Visit(nullabilityHandled);

            var fromSqlParameterOptimized = new FromSqlParameterApplyingExpressionVisitor(
                Dependencies.SqlExpressionFactory,
                Dependencies.ParameterNameGeneratorFactory.Create(),
                parametersValues).Visit(nullParametersOptimized);

            if (!ReferenceEquals(nullParametersOptimized, fromSqlParameterOptimized))
            {
                canCache = false;
            }

            return (selectExpression: (SelectExpression)fromSqlParameterOptimized, canCache);
        }
    }
}
