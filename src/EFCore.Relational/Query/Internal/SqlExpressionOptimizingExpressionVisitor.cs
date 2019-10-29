// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public class SqlExpressionOptimizingExpressionVisitor : ExpressionVisitor
    {
        private readonly bool _useRelationalNulls;

        private static bool TryNegate(ExpressionType expressionType, out ExpressionType result)
        {
            var negated = expressionType switch {
                ExpressionType.AndAlso            => ExpressionType.OrElse,
                ExpressionType.OrElse             => ExpressionType.AndAlso,
                ExpressionType.Equal              => ExpressionType.NotEqual,
                ExpressionType.NotEqual           => ExpressionType.Equal,
                ExpressionType.GreaterThan        => ExpressionType.LessThanOrEqual,
                ExpressionType.GreaterThanOrEqual => ExpressionType.LessThan,
                ExpressionType.LessThan           => ExpressionType.GreaterThanOrEqual,
                ExpressionType.LessThanOrEqual    => ExpressionType.GreaterThan,
                _ => (ExpressionType?)null
            };

            result = negated ?? default;

            return negated.HasValue;
        }

        public SqlExpressionOptimizingExpressionVisitor(ISqlExpressionFactory sqlExpressionFactory, bool useRelationalNulls)
        {
            SqlExpressionFactory = sqlExpressionFactory;
            _useRelationalNulls = useRelationalNulls;
        }

        protected virtual ISqlExpressionFactory SqlExpressionFactory { get; }

        protected override Expression VisitExtension(Expression extensionExpression)
            => extensionExpression switch
                {
                    SqlUnaryExpression sqlUnaryExpression => VisitSqlUnaryExpression(sqlUnaryExpression),
                    SqlBinaryExpression sqlBinaryExpression => VisitSqlBinaryExpression(sqlBinaryExpression),
                    _ => base.VisitExtension(extensionExpression),
                };

        protected virtual Expression VisitSqlUnaryExpression(SqlUnaryExpression sqlUnaryExpression)
        {
            if (sqlUnaryExpression.OperatorType == ExpressionType.Not)
            {
                return VisitNot(sqlUnaryExpression);
            }

            var newOperand = (SqlExpression)Visit(sqlUnaryExpression.Operand);

            switch (sqlUnaryExpression.OperatorType)
            {
                //case ExpressionType.Not:
                //    return VisitNot(sqlUnaryExpression);

                case ExpressionType.Equal:
                    //switch (newOperand)
                    //{
                    //    case SqlConstantExpression constantOperand:
                    //        return SqlExpressionFactory.Constant(constantOperand.Value == null, sqlUnaryExpression.TypeMapping);

                    //    case ColumnExpression columnOperand
                    //    when !columnOperand.IsNullable:
                    //        return SqlExpressionFactory.Constant(false, sqlUnaryExpression.TypeMapping);

                    //    case SqlUnaryExpression sqlUnaryOperand
                    //    when sqlUnaryOperand.OperatorType == ExpressionType.Convert
                    //        || sqlUnaryOperand.OperatorType == ExpressionType.Not
                    //        || sqlUnaryOperand.OperatorType == ExpressionType.Negate:
                    //        return (SqlExpression)Visit(SqlExpressionFactory.IsNull(newOperand));

                    //    case SqlBinaryExpression sqlBinaryOperand:
                    //        var newLeft = (SqlExpression)Visit(SqlExpressionFactory.IsNull(sqlBinaryOperand.Left));
                    //        var newRight = (SqlExpression)Visit(SqlExpressionFactory.IsNull(sqlBinaryOperand.Right));

                    //        return sqlBinaryOperand.OperatorType == ExpressionType.Coalesce
                    //            ? SimplifyLogicalSqlBinaryExpression(ExpressionType.AndAlso, newLeft, newRight, sqlBinaryOperand.TypeMapping)
                    //            : SimplifyLogicalSqlBinaryExpression(ExpressionType.OrElse, newLeft, newRight, sqlBinaryOperand.TypeMapping);
                    //}
                    //break;

                case ExpressionType.NotEqual:
                    return SimplifyNullNotNullExpression(
                        sqlUnaryExpression.OperatorType,
                        newOperand,
                        sqlUnaryExpression.Type,
                        sqlUnaryExpression.TypeMapping);
                    //switch (newOperand)
                    //{
                    //    case SqlConstantExpression constantOperand:
                    //        return SqlExpressionFactory.Constant(constantOperand.Value != null, sqlUnaryExpression.TypeMapping);

                    //    case ColumnExpression columnOperand
                    //    when !columnOperand.IsNullable:
                    //        return SqlExpressionFactory.Constant(true, sqlUnaryExpression.TypeMapping);

                    //    case SqlUnaryExpression sqlUnaryOperand
                    //    when sqlUnaryOperand.OperatorType == ExpressionType.Convert
                    //        || sqlUnaryOperand.OperatorType == ExpressionType.Not
                    //        || sqlUnaryOperand.OperatorType == ExpressionType.Negate:
                    //        return (SqlExpression)Visit(SqlExpressionFactory.IsNotNull(newOperand));

                    //    case SqlBinaryExpression sqlBinaryOperand:
                    //        var newLeft = (SqlExpression)Visit(SqlExpressionFactory.IsNotNull(sqlBinaryOperand.Left));
                    //        var newRight = (SqlExpression)Visit(SqlExpressionFactory.IsNotNull(sqlBinaryOperand.Right));

                    //        return sqlBinaryOperand.OperatorType == ExpressionType.Coalesce
                    //            ? SimplifyLogicalSqlBinaryExpression(ExpressionType.OrElse, newLeft, newRight, sqlBinaryOperand.TypeMapping)
                    //            : SimplifyLogicalSqlBinaryExpression(ExpressionType.AndAlso, newLeft, newRight, sqlBinaryOperand.TypeMapping);
                    //}
                    //break;
            }

            return sqlUnaryExpression.Update(newOperand);
        }

        private SqlExpression SimplifyNullNotNullExpression(
            ExpressionType operatorType,
            SqlExpression operand,
            Type type,
            RelationalTypeMapping typeMapping)
        {
            switch (operatorType)
            {
                case ExpressionType.Equal:
                    switch (operand)
                    {
                        case SqlConstantExpression constantOperand:
                            return SqlExpressionFactory.Constant(constantOperand.Value == null, typeMapping);

                        case ColumnExpression columnOperand
                        when !columnOperand.IsNullable:
                            return SqlExpressionFactory.Constant(false, typeMapping);

                        case SqlUnaryExpression sqlUnaryOperand
                        when sqlUnaryOperand.OperatorType == ExpressionType.Convert
                            || sqlUnaryOperand.OperatorType == ExpressionType.Not
                            || sqlUnaryOperand.OperatorType == ExpressionType.Negate:
                            //return (SqlExpression)Visit(SqlExpressionFactory.IsNull(operand));
                            return SqlExpressionFactory.IsNull(sqlUnaryOperand.Operand);

                        case SqlBinaryExpression sqlBinaryOperand:
                            var newLeft = SimplifyNullNotNullExpression(ExpressionType.Equal, sqlBinaryOperand.Left, typeof(bool), typeMapping/*: null*/);
                            var newRight = SimplifyNullNotNullExpression(ExpressionType.Equal, sqlBinaryOperand.Right, typeof(bool), typeMapping/*: null*/);
                            //var leftIsNull = SqlExpressionFactory.IsNull(sqlBinaryOperand.Left);
                            //var rightIsNull = SqlExpressionFactory.IsNull(sqlBinaryOperand.Right);
                            //var newLeft = SimplifyNullNotNullExpression(leftIsNull.OperatorType, leftIsNull.Operand, leftIsNull.Type, leftIsNull.TypeMapping);
                            //var newRight = SimplifyNullNotNullExpression(rightIsNull.OperatorType, rightIsNull.Operand, rightIsNull.Type, rightIsNull.TypeMapping);
                            //var newLeft = (SqlExpression)Visit(SqlExpressionFactory.IsNull(sqlBinaryOperand.Left));
                            //var newRight = (SqlExpression)Visit(SqlExpressionFactory.IsNull(sqlBinaryOperand.Right));

                            return sqlBinaryOperand.OperatorType == ExpressionType.Coalesce
                                ? SimplifyLogicalSqlBinaryExpression(ExpressionType.AndAlso, newLeft, newRight, /*sqlBinaryOperand.TypeMapping*/typeMapping)
                                : SimplifyLogicalSqlBinaryExpression(ExpressionType.OrElse, newLeft, newRight, /*sqlBinaryOperand.TypeMapping*/typeMapping);
                    }
                    break;

                case ExpressionType.NotEqual:
                    switch (operand)
                    {
                        case SqlConstantExpression constantOperand:
                            return SqlExpressionFactory.Constant(constantOperand.Value != null, typeMapping);

                        case ColumnExpression columnOperand
                        when !columnOperand.IsNullable:
                            return SqlExpressionFactory.Constant(true, typeMapping);

                        case SqlUnaryExpression sqlUnaryOperand
                        when sqlUnaryOperand.OperatorType == ExpressionType.Convert
                            || sqlUnaryOperand.OperatorType == ExpressionType.Not
                            || sqlUnaryOperand.OperatorType == ExpressionType.Negate:
                            //return (SqlExpression)Visit(SqlExpressionFactory.IsNotNull(operand));
                            return SqlExpressionFactory.IsNotNull(sqlUnaryOperand.Operand);

                        case SqlBinaryExpression sqlBinaryOperand:
                            var newLeft = SimplifyNullNotNullExpression(ExpressionType.NotEqual, sqlBinaryOperand.Left, typeof(bool), typeMapping: null);
                            var newRight = SimplifyNullNotNullExpression(ExpressionType.NotEqual, sqlBinaryOperand.Right, typeof(bool), typeMapping: null);
                            //var newLeft = (SqlExpression)Visit(SqlExpressionFactory.IsNotNull(sqlBinaryOperand.Left));
                            //var newRight = (SqlExpression)Visit(SqlExpressionFactory.IsNotNull(sqlBinaryOperand.Right));

                            return sqlBinaryOperand.OperatorType == ExpressionType.Coalesce
                                ? SimplifyLogicalSqlBinaryExpression(ExpressionType.OrElse, newLeft, newRight, sqlBinaryOperand.TypeMapping)
                                : SimplifyLogicalSqlBinaryExpression(ExpressionType.AndAlso, newLeft, newRight, sqlBinaryOperand.TypeMapping);
                    }
                    break;
            }

            return SqlExpressionFactory.MakeUnary(operatorType, operand, type, typeMapping);
        }

        protected virtual Expression VisitNot(SqlUnaryExpression sqlUnaryExpression)
        {
            // !(true) -> false
            // !(false) -> true
            if (sqlUnaryExpression.Operand is SqlConstantExpression innerConstantBool
                && innerConstantBool.Value is bool value)
            {
                return SqlExpressionFactory.Constant(!value, sqlUnaryExpression.TypeMapping);
            }

            if (sqlUnaryExpression.Operand is InExpression inExpression)
            {
                return Visit(inExpression.Negate());
            }

            if (sqlUnaryExpression.Operand is SqlUnaryExpression innerUnary)
            {
                // !(!a) -> a
                if (innerUnary.OperatorType == ExpressionType.Not)
                {
                    return Visit(innerUnary.Operand);
                }

                if (innerUnary.OperatorType == ExpressionType.Equal)
                {
                    //!(a IS NULL) -> a IS NOT NULL
                    return Visit(SqlExpressionFactory.IsNotNull(innerUnary.Operand));
                }

                //!(a IS NOT NULL) -> a IS NULL
                if (innerUnary.OperatorType == ExpressionType.NotEqual)
                {
                    return Visit(SqlExpressionFactory.IsNull(innerUnary.Operand));
                }
            }

            if (sqlUnaryExpression.Operand is SqlBinaryExpression innerBinary)
            {
                // De Morgan's
                if (innerBinary.OperatorType == ExpressionType.AndAlso
                    || innerBinary.OperatorType == ExpressionType.OrElse)
                {
                    var newLeft = (SqlExpression)Visit(SqlExpressionFactory.Not(innerBinary.Left));
                    var newRight = (SqlExpression)Visit(SqlExpressionFactory.Not(innerBinary.Right));

                    return SimplifyLogicalSqlBinaryExpression(
                        innerBinary.OperatorType == ExpressionType.AndAlso
                            ? ExpressionType.OrElse
                            : ExpressionType.AndAlso,
                        newLeft,
                        newRight,
                        innerBinary.TypeMapping);
                }

                // those optimizations are only valid in 2-value logic
                // they are safe to do here because null semantics removes possibility of nulls in the tree
                // however if we decide to do "partial" null semantics (that doesn't distinguish between NULL and FALSE, e.g. for predicates)
                // we need to be extra careful here
                if (!_useRelationalNulls && TryNegate(innerBinary.OperatorType, out var negated))
                {
                    return Visit(
                        SqlExpressionFactory.MakeBinary(
                            negated,
                            innerBinary.Left,
                            innerBinary.Right,
                            innerBinary.TypeMapping));
                }
            }

            var newOperand = (SqlExpression)Visit(sqlUnaryExpression.Operand);

            return sqlUnaryExpression.Update(newOperand);
        }

        protected virtual Expression VisitSqlBinaryExpression(SqlBinaryExpression sqlBinaryExpression)
        {
            var newLeft = (SqlExpression)Visit(sqlBinaryExpression.Left);
            var newRight = (SqlExpression)Visit(sqlBinaryExpression.Right);

            if (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                || sqlBinaryExpression.OperatorType == ExpressionType.OrElse)
            {
                var leftUnary = newLeft as SqlUnaryExpression;
                var rightUnary = newRight as SqlUnaryExpression;
                if (leftUnary != null
                    && rightUnary != null
                    && (leftUnary.OperatorType == ExpressionType.Equal || leftUnary.OperatorType == ExpressionType.NotEqual)
                    && (rightUnary.OperatorType == ExpressionType.Equal || rightUnary.OperatorType == ExpressionType.NotEqual)
                    && leftUnary.Operand == rightUnary.Operand)
                {
                    return leftUnary.OperatorType == rightUnary.OperatorType
                        ? (SqlExpression)leftUnary
                        : SqlExpressionFactory.Constant(sqlBinaryExpression.OperatorType == ExpressionType.OrElse, sqlBinaryExpression.TypeMapping);
                }

                return SimplifyLogicalSqlBinaryExpression(
                    sqlBinaryExpression.OperatorType,
                    newLeft,
                    newRight,
                    sqlBinaryExpression.TypeMapping);
            }

            if (sqlBinaryExpression.OperatorType == ExpressionType.Equal
                || sqlBinaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                var leftNullConstant = newLeft is SqlConstantExpression leftConstant && leftConstant.Value == null;
                var rightNullConstant = newRight is SqlConstantExpression rightConstant && rightConstant.Value == null;
                if (leftNullConstant || rightNullConstant)
                {
                    return SimplifyNullComparisonExpression(
                        sqlBinaryExpression.OperatorType,
                        newLeft,
                        newRight,
                        leftNullConstant,
                        rightNullConstant,
                        sqlBinaryExpression.TypeMapping);
                }

                var leftBoolConstant = newLeft.Type == typeof(bool) ? newLeft as SqlConstantExpression : null;
                var rightBoolConstant = newRight.Type == typeof(bool) ? newRight as SqlConstantExpression : null;
                if (leftBoolConstant != null || rightBoolConstant != null)
                {
                    return SimplifyBoolConstantComparisonExpression(
                        sqlBinaryExpression.OperatorType,
                        newLeft,
                        newRight,
                        leftBoolConstant,
                        rightBoolConstant,
                        sqlBinaryExpression.TypeMapping);
                }
            }

            return sqlBinaryExpression.Update(newLeft, newRight);
        }

        protected SqlExpression SimplifyNullComparisonExpression(
            ExpressionType operatorType,
            SqlExpression left,
            SqlExpression right,
            bool leftNull,
            bool rightNull,
            RelationalTypeMapping typeMapping)
        {
            if ((operatorType == ExpressionType.Equal || operatorType == ExpressionType.NotEqual)
                && (leftNull || rightNull))
            {
                if (leftNull && rightNull)
                {
                    return SqlExpressionFactory.Constant(operatorType == ExpressionType.Equal, typeMapping);
                }

                if (leftNull)
                {
                    return SimplifyNullNotNullExpression(operatorType, right, typeof(bool), typeMapping);

                    //return operatorType == ExpressionType.Equal
                    //    ? SqlExpressionFactory.IsNull(right)
                    //    : SqlExpressionFactory.IsNotNull(right);
                }

                if (rightNull)
                {
                    return SimplifyNullNotNullExpression(operatorType, left, typeof(bool), typeMapping);

                    //return operatorType == ExpressionType.Equal
                    //    ? SqlExpressionFactory.IsNull(left)
                    //    : SqlExpressionFactory.IsNotNull(left);
                }
            }

            return SqlExpressionFactory.MakeBinary(operatorType, left, right, typeMapping);
        }

        private SqlExpression SimplifyBoolConstantComparisonExpression(
            ExpressionType operatorType,
            SqlExpression left,
            SqlExpression right,
            SqlConstantExpression leftBoolConstant,
            SqlConstantExpression rightBoolConstant,
            RelationalTypeMapping typeMapping)
        {
            if (leftBoolConstant != null && rightBoolConstant != null)
            {
                // can't optimize this because it reverts search conditions back to values
                // we could do that IF the visitor doesn't need to run after search condition converter for SqlServer
                return SqlExpressionFactory.MakeBinary(operatorType, left, right, typeMapping);

                //return operatorType == ExpressionType.Equal
                //    ? SqlExpressionFactory.Constant((bool)leftBoolConstant.Value == (bool)rightBoolConstant.Value, typeMapping)
                //    : SqlExpressionFactory.Constant((bool)leftBoolConstant.Value != (bool)rightBoolConstant.Value, typeMapping);
            }

            if (leftBoolConstant != null
                && right is SqlUnaryExpression rightUnary
                && (rightUnary.OperatorType == ExpressionType.Equal || rightUnary.OperatorType == ExpressionType.NotEqual))
            {
                // true == a is null -> a is null
                // true == a is not null -> a is not null
                // false == a is null -> a is not null
                // false == a is not null -> a is null
                // true != a is null -> a is not null
                // true != a is not null -> a is null
                // false != a is null -> a is null
                // false != a is not null -> a is not null
                return operatorType == ExpressionType.Equal
                    ? (bool)leftBoolConstant.Value
                        ? rightUnary.OperatorType == ExpressionType.Equal
                            ? SqlExpressionFactory.IsNull(rightUnary.Operand)
                            : SqlExpressionFactory.IsNotNull(rightUnary.Operand)
                        : rightUnary.OperatorType == ExpressionType.Equal
                            ? SqlExpressionFactory.IsNotNull(rightUnary.Operand)
                            : SqlExpressionFactory.IsNull(rightUnary.Operand)
                    : !(bool)leftBoolConstant.Value
                        ? rightUnary.OperatorType == ExpressionType.Equal
                            ? SqlExpressionFactory.IsNotNull(rightUnary.Operand)
                            : SqlExpressionFactory.IsNull(rightUnary.Operand)
                        : rightUnary.OperatorType == ExpressionType.Equal
                            ? SqlExpressionFactory.IsNull(rightUnary.Operand)
                            : SqlExpressionFactory.IsNotNull(rightUnary.Operand);
            }

            if (rightBoolConstant != null
                && left is SqlUnaryExpression leftUnary
                && (leftUnary.OperatorType == ExpressionType.Equal || leftUnary.OperatorType == ExpressionType.NotEqual))
            {
                // a is null == true -> a is null
                // a is not null == true -> a is not null
                // a is null == false -> a is not null
                // a is not null == false -> a is null
                // a is null != true -> a is not null
                // a is not null != true -> a is null
                // a is null != false -> a is null
                // a is not null != false -> a is not null
                return operatorType == ExpressionType.Equal
                    ? (bool)rightBoolConstant.Value
                        ? leftUnary.OperatorType == ExpressionType.Equal
                            ? SqlExpressionFactory.IsNull(leftUnary.Operand)
                            : SqlExpressionFactory.IsNotNull(leftUnary.Operand)
                        : leftUnary.OperatorType == ExpressionType.Equal
                            ? SqlExpressionFactory.IsNotNull(leftUnary.Operand)
                            : SqlExpressionFactory.IsNull(leftUnary.Operand)
                    : !(bool)rightBoolConstant.Value
                        ? leftUnary.OperatorType == ExpressionType.Equal
                            ? SqlExpressionFactory.IsNotNull(leftUnary.Operand)
                            : SqlExpressionFactory.IsNull(leftUnary.Operand)
                        : leftUnary.OperatorType == ExpressionType.Equal
                            ? SqlExpressionFactory.IsNull(leftUnary.Operand)
                            : SqlExpressionFactory.IsNotNull(leftUnary.Operand);
            }

            return SqlExpressionFactory.MakeBinary(operatorType, left, right, typeMapping);
        }

        private SqlExpression SimplifyLogicalSqlBinaryExpression(
            ExpressionType operatorType,
            SqlExpression left,
            SqlExpression right,
            RelationalTypeMapping typeMapping)
        {
            // true && a -> a
            // true || a -> true
            // false && a -> false
            // false || a -> a
            if (left is SqlConstantExpression newLeftConstant)
            {
                return operatorType == ExpressionType.AndAlso
                    ? (bool)newLeftConstant.Value
                        ? right
                        : newLeftConstant
                    : (bool)newLeftConstant.Value
                        ? newLeftConstant
                        : right;
            }
            else if (right is SqlConstantExpression newRightConstant)
            {
                // a && true -> a
                // a || true -> true
                // a && false -> false
                // a || false -> a
                return operatorType == ExpressionType.AndAlso
                    ? (bool)newRightConstant.Value
                        ? left
                        : newRightConstant
                    : (bool)newRightConstant.Value
                        ? newRightConstant
                        : left;
            }

            return SqlExpressionFactory.MakeBinary(operatorType, left, right, typeMapping);
        }
    }
}
