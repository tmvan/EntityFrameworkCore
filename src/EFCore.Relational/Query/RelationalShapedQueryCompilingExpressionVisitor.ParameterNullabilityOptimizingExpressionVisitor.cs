// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Microsoft.EntityFrameworkCore.Query
{
    public partial class RelationalShapedQueryCompilingExpressionVisitor
    {
        private class ParameterNullabilityOptimizingExpressionVisitor : ExpressionVisitor
        {
            private readonly ISqlExpressionFactory _sqlExpressionFactory;
            private readonly IReadOnlyDictionary<string, object> _parametersValues;

            public ParameterNullabilityOptimizingExpressionVisitor(
                ISqlExpressionFactory sqlExpressionFactory, IReadOnlyDictionary<string, object> parametersValues)
            {
                _sqlExpressionFactory = sqlExpressionFactory;
                _parametersValues = parametersValues;
            }

            protected override Expression VisitExtension(Expression expression)
            {
                if (expression is SqlUnaryExpression sqlUnaryExpression)
                {
                    var newOperand = (SqlExpression)Visit(sqlUnaryExpression.Operand);
                    if (newOperand is SqlParameterExpression parameterOperand)
                    {
                        var parameterValue = _parametersValues[parameterOperand.Name];
                        if (sqlUnaryExpression.OperatorType == ExpressionType.Equal)
                        {
                            return _sqlExpressionFactory.Constant(parameterValue == null, sqlUnaryExpression.TypeMapping);
                        }

                        if (sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
                        {
                            return _sqlExpressionFactory.Constant(parameterValue != null, sqlUnaryExpression.TypeMapping);
                        }
                    }

                    if (newOperand is SqlConstantExpression constantOperand)
                    {
                        var constantValue = constantOperand.Value;
                        if (sqlUnaryExpression.OperatorType == ExpressionType.Equal)
                        {
                            return _sqlExpressionFactory.Constant(constantValue == null, sqlUnaryExpression.TypeMapping);
                        }

                        if (sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
                        {
                            return _sqlExpressionFactory.Constant(constantValue != null, sqlUnaryExpression.TypeMapping);
                        }

                        if (sqlUnaryExpression.OperatorType == ExpressionType.Not)
                        {
                            return _sqlExpressionFactory.Constant(!((bool)constantValue), sqlUnaryExpression.TypeMapping);
                        }
                    }

                    return sqlUnaryExpression.Update(newOperand);
                }

                if (expression is SqlBinaryExpression sqlBinaryExpression)
                {
                    var newLeft = (SqlExpression)Visit(sqlBinaryExpression.Left);
                    var newRight = (SqlExpression)Visit(sqlBinaryExpression.Right);

                    if (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso)
                    {
                        if (newLeft is SqlConstantExpression leftConstant)
                        {
                            return (bool)leftConstant.Value
                                ? newRight
                                : _sqlExpressionFactory.Constant(false, sqlBinaryExpression.TypeMapping);
                        }

                        if (newRight is SqlConstantExpression rightConstant)
                        {
                            return (bool)rightConstant.Value
                                ? newLeft
                                : _sqlExpressionFactory.Constant(false, sqlBinaryExpression.TypeMapping);
                        }

                    }

                    if (sqlBinaryExpression.OperatorType == ExpressionType.OrElse)
                    {
                        if (newLeft is SqlConstantExpression leftConstant)
                        {
                            return (bool)leftConstant.Value
                                ? _sqlExpressionFactory.Constant(true, sqlBinaryExpression.TypeMapping)
                                : newRight;
                        }

                        if (newRight is SqlConstantExpression rightConstant)
                        {
                            return (bool)rightConstant.Value
                                ? _sqlExpressionFactory.Constant(true, sqlBinaryExpression.TypeMapping)
                                : newLeft;
                        }
                    }
                }

                return base.VisitExtension(expression);
            }
        }
    }
}
