// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.SqlServer.Query.Internal
{
    public class SqlServerParameterBasedQueryTranslationPostprocessor : RelationalParameterBasedQueryTranslationPostprocessor
    {
        public SqlServerParameterBasedQueryTranslationPostprocessor(
            [NotNull] RelationalParameterBasedQueryTranslationPostprocessorDependencies dependencies,
            bool useRelationalNulls)
            : base(dependencies, useRelationalNulls)
        {
        }

        public override (SelectExpression selectExpression, bool canCache) Optimize(
            SelectExpression selectExpression,
            IReadOnlyDictionary<string, object> parametersValues)
        {
            Check.NotNull(selectExpression, nameof(selectExpression));
            Check.NotNull(parametersValues, nameof(parametersValues));

            var (optimizedSelectExpression, canCache) = base.Optimize(selectExpression, parametersValues);

            var searchConditionOptimized = (SelectExpression)new SearchConditionConvertingExpressionVisitor(
                Dependencies.SqlExpressionFactory).Visit(optimizedSelectExpression);

            var optimized = (SelectExpression)new SqlServerSqlExpressionOptimizingExpressionVisitor(
                Dependencies.SqlExpressionFactory, UseRelationalNulls, parametersValues).Visit(searchConditionOptimized);

            return (optimized, canCache);
        }

        private sealed class SqlServerSqlExpressionOptimizingExpressionVisitor : SqlExpressionOptimizingExpressionVisitor
        {
            public SqlServerSqlExpressionOptimizingExpressionVisitor(
                [NotNull] ISqlExpressionFactory sqlExpressionFactory,
                bool useRelationalNulls,
                [NotNull] IReadOnlyDictionary<string, object> parametersValues)
                : base(sqlExpressionFactory, useRelationalNulls, parametersValues)
            {
            }

            protected override Expression VisitSqlBinaryExpression(SqlBinaryExpression sqlBinaryExpression)
            {
                var result = base.VisitSqlBinaryExpression(sqlBinaryExpression);
                if (result is SqlBinaryExpression sqlBinaryResult
                    && sqlBinaryResult.OperatorType == ExpressionType.Equal
                    && sqlBinaryResult.Left is SqlConstantExpression leftConstant
                    && leftConstant.Value is bool leftValue
                    && sqlBinaryResult.Right is SqlConstantExpression rightConstant
                    && rightConstant.Value is bool rightValue)
                {
                    // true == true -> 1 == 1
                    // false == true -> 0 == 1
                    // to avoid CAST(1 as bit)
                    return leftValue == rightValue
                        ? SqlExpressionFactory.Equal(
                            SqlExpressionFactory.Constant(1),
                            SqlExpressionFactory.Constant(1))
                        : SqlExpressionFactory.Equal(
                            SqlExpressionFactory.Constant(0),
                            SqlExpressionFactory.Constant(1));
                }

                return result;
            }
        }
    }
}
