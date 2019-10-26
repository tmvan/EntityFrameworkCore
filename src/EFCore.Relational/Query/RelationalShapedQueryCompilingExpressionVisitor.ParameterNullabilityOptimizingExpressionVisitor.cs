// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Microsoft.EntityFrameworkCore.Query
{
    public partial class RelationalShapedQueryCompilingExpressionVisitor
    {
        private class ParameterNullabilityBasedSqlExpressionOptimizingExpressionVisitor : SqlExpressionOptimizingExpressionVisitor
        {
            private readonly IReadOnlyDictionary<string, object> _parametersValues;

            public ParameterNullabilityBasedSqlExpressionOptimizingExpressionVisitor(
                ISqlExpressionFactory sqlExpressionFactory,
                bool useRelationalNulls,
                IReadOnlyDictionary<string, object> parametersValues)
                : base(sqlExpressionFactory, useRelationalNulls)
            {
                _parametersValues = parametersValues;
            }

            protected override Expression VisitExtension(Expression extensionExpression)
            {
                if (extensionExpression is SelectExpression selectExpression)
                {
                    var newSelectExpression = (SelectExpression)base.VisitExtension(extensionExpression);

                    // if predicate is optimized to true, we can simply remove it
                    var newPredicate = newSelectExpression.Predicate is SqlConstantExpression newSelectPredicateConstant
                        && !(selectExpression.Predicate is SqlConstantExpression)
                        ? (bool)newSelectPredicateConstant.Value
                            ? null
                            : SqlExpressionFactory.Equal(
                                newSelectPredicateConstant,
                                SqlExpressionFactory.Constant(true, newSelectPredicateConstant.TypeMapping))
                        : newSelectExpression.Predicate;

                    var newHaving = newSelectExpression.Having is SqlConstantExpression newSelectHavingConstant
                        && !(selectExpression.Having is SqlConstantExpression)
                        ? (bool)newSelectHavingConstant.Value
                            ? null
                            : SqlExpressionFactory.Equal(
                                newSelectHavingConstant,
                                SqlExpressionFactory.Constant(true, newSelectHavingConstant.TypeMapping))
                        : newSelectExpression.Having;

                    return newPredicate != newSelectExpression.Predicate
                        || newHaving != newSelectExpression.Having
                        ? newSelectExpression.Update(
                            newSelectExpression.Projection.ToList(),
                            newSelectExpression.Tables.ToList(),
                            newPredicate,
                            newSelectExpression.GroupBy.ToList(),
                            newHaving,
                            newSelectExpression.Orderings.ToList(),
                            newSelectExpression.Limit,
                            newSelectExpression.Offset,
                            newSelectExpression.IsDistinct,
                            newSelectExpression.Alias)
                        : newSelectExpression;
                }

                return base.VisitExtension(extensionExpression);
            }

            protected override Expression VisitSqlUnaryExpression(SqlUnaryExpression sqlUnaryExpression)
            {
                var result = base.VisitSqlUnaryExpression(sqlUnaryExpression);
                if (result is SqlUnaryExpression newUnaryExpresion
                    && newUnaryExpresion.Operand is SqlParameterExpression parameterOperand)
                {
                    var parameterValue = _parametersValues[parameterOperand.Name];
                    if (sqlUnaryExpression.OperatorType == ExpressionType.Equal)
                    {
                        return SqlExpressionFactory.Constant(parameterValue == null, sqlUnaryExpression.TypeMapping);
                    }

                    if (sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
                    {
                        return SqlExpressionFactory.Constant(parameterValue != null, sqlUnaryExpression.TypeMapping);
                    }
                }

                return result;
            }

            protected override Expression VisitSqlBinaryExpression(SqlBinaryExpression sqlBinaryExpression)
            {
                var result = base.VisitSqlBinaryExpression(sqlBinaryExpression);
                if (result is SqlBinaryExpression sqlBinaryResult)
                {
                    var leftNullParameter = sqlBinaryResult.Left is SqlParameterExpression leftParameter
                        && _parametersValues[leftParameter.Name] == null;

                    var rightNullParameter = sqlBinaryResult.Right is SqlParameterExpression rightParameter
                        && _parametersValues[rightParameter.Name] == null;

                    if ((sqlBinaryResult.OperatorType == ExpressionType.Equal || sqlBinaryResult.OperatorType == ExpressionType.NotEqual)
                        && (leftNullParameter || rightNullParameter))
                    {
                        return SimplifyNullComparisonExpression(
                            sqlBinaryResult.OperatorType,
                            sqlBinaryResult.Left,
                            sqlBinaryResult.Right,
                            leftNullParameter,
                            rightNullParameter,
                            sqlBinaryResult.TypeMapping);
                    }
                }

                return result;
            }
        }
    }
}
