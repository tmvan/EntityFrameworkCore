// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public class NullSemanticsRewritingExpressionVisitor : NullabilityComputingSqlExpressionVisitor
    {
        private readonly ISqlExpressionFactory _sqlExpressionFactory;
        private readonly bool _useRelationalNulls;
        private bool _canOptimize;
        private readonly List<ColumnExpression> _nonNullableColumns = new List<ColumnExpression>();
        private readonly IReadOnlyDictionary<string, object> _parametersValues;

        public NullSemanticsRewritingExpressionVisitor(
            ISqlExpressionFactory sqlExpressionFactory,
            bool useRelationalNulls,
            IReadOnlyDictionary<string, object> parametersValues)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
            _useRelationalNulls = useRelationalNulls;
            _parametersValues = parametersValues;
            _canOptimize = true;
        }

        protected override Expression VisitCase(CaseExpression caseExpression)
        {
            Nullable = false;
            // if there is no 'else' there is a possibility of null, when none of the conditions are met
            // otherwise the result is nullable if any of the WhenClause results OR ElseResult is nullable
            var isNullable = caseExpression.ElseResult == null;

            var canOptimize = _canOptimize;
            var testIsCondition = caseExpression.Operand == null;
            _canOptimize = false;
            var newOperand = (SqlExpression)Visit(caseExpression.Operand);
            var newWhenClauses = new List<CaseWhenClause>();
            foreach (var whenClause in caseExpression.WhenClauses)
            {
                _canOptimize = testIsCondition;
                var newTest = (SqlExpression)Visit(whenClause.Test);
                _canOptimize = false;
                Nullable = false;
                var newResult = (SqlExpression)Visit(whenClause.Result);
                isNullable |= Nullable;
                newWhenClauses.Add(new CaseWhenClause(newTest, newResult));
            }

            _canOptimize = false;
            var newElseResult = (SqlExpression)Visit(caseExpression.ElseResult);
            Nullable |= isNullable;
            _canOptimize = canOptimize;

            return caseExpression.Update(newOperand, newWhenClauses, newElseResult);
        }

        protected override Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
            => CantOptimize(base.VisitCrossApply, crossApplyExpression);

        protected override Expression VisitCrossJoin(CrossJoinExpression crossJoinExpression)
            => CantOptimize(base.VisitCrossJoin, crossJoinExpression);

        protected override Expression VisitExcept(ExceptExpression exceptExpression)
            => CantOptimize(base.VisitExcept, exceptExpression);

        protected override Expression VisitExists(ExistsExpression existsExpression)
            => CantOptimize(base.VisitExists, existsExpression);

        protected override Expression VisitIn(InExpression inExpression)
            => CantOptimize(base.VisitIn, inExpression);

        protected override Expression VisitIntersect(IntersectExpression intersectExpression)
            => CantOptimize(base.VisitIntersect, intersectExpression);

        protected override Expression VisitLike(LikeExpression likeExpression)
            => CantOptimize(base.VisitLike, likeExpression);

        protected override Expression VisitInnerJoin(InnerJoinExpression innerJoinExpression)
        {
            var canOptimize = _canOptimize;
            _canOptimize = false;
            var newTable = (TableExpressionBase)Visit(innerJoinExpression.Table);
            var newJoinPredicate = VisitJoinPredicate((SqlBinaryExpression)innerJoinExpression.JoinPredicate);
            _canOptimize = canOptimize;

            return innerJoinExpression.Update(newTable, newJoinPredicate);
        }

        protected override Expression VisitLeftJoin(LeftJoinExpression leftJoinExpression)
        {
            var canOptimize = _canOptimize;
            _canOptimize = false;
            var newTable = (TableExpressionBase)Visit(leftJoinExpression.Table);
            var newJoinPredicate = VisitJoinPredicate((SqlBinaryExpression)leftJoinExpression.JoinPredicate);
            _canOptimize = canOptimize;

            return leftJoinExpression.Update(newTable, newJoinPredicate);
        }

        private SqlExpression VisitJoinPredicate(SqlBinaryExpression predicate)
        {
            var canOptimize = _canOptimize;
            _canOptimize = true;

            if (predicate.OperatorType == ExpressionType.Equal)
            {
                var newLeft = (SqlExpression)Visit(predicate.Left);
                var newRight = (SqlExpression)Visit(predicate.Right);
                _canOptimize = canOptimize;

                return predicate.Update(newLeft, newRight);
            }

            if (predicate.OperatorType == ExpressionType.AndAlso)
            {
                var newPredicate = (SqlExpression)VisitSqlBinary(predicate);
                _canOptimize = canOptimize;

                return newPredicate;
            }

            throw new InvalidOperationException("Unexpected join predicate shape: " + predicate);
        }

        protected override Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
            => CantOptimize(base.VisitOuterApply, outerApplyExpression);

        protected override Expression VisitRowNumber(RowNumberExpression rowNumberExpression)
            => CantOptimize(base.VisitRowNumber, rowNumberExpression);

        protected override Expression VisitScalarSubquery(ScalarSubqueryExpression scalarSubqueryExpression)
            => CantOptimize(base.VisitScalarSubquery, scalarSubqueryExpression);

        protected override Expression VisitSelect(SelectExpression selectExpression)
        {
            var changed = false;
            var canOptimize = _canOptimize;
            var projections = new List<ProjectionExpression>();
            _canOptimize = false;
            foreach (var item in selectExpression.Projection)
            {
                var updatedProjection = (ProjectionExpression)Visit(item);
                projections.Add(updatedProjection);
                changed |= updatedProjection != item;
            }

            var tables = new List<TableExpressionBase>();
            foreach (var table in selectExpression.Tables)
            {
                var newTable = (TableExpressionBase)Visit(table);
                changed |= newTable != table;
                tables.Add(newTable);
            }

            _canOptimize = true;
            var predicate = (SqlExpression)Visit(selectExpression.Predicate);
            changed |= predicate != selectExpression.Predicate;

            var groupBy = new List<SqlExpression>();
            _canOptimize = false;
            foreach (var groupingKey in selectExpression.GroupBy)
            {
                var newGroupingKey = (SqlExpression)Visit(groupingKey);
                changed |= newGroupingKey != groupingKey;
                groupBy.Add(newGroupingKey);
            }

            _canOptimize = true;
            var havingExpression = (SqlExpression)Visit(selectExpression.Having);
            changed |= havingExpression != selectExpression.Having;

            var orderings = new List<OrderingExpression>();
            _canOptimize = false;
            foreach (var ordering in selectExpression.Orderings)
            {
                var orderingExpression = (SqlExpression)Visit(ordering.Expression);
                changed |= orderingExpression != ordering.Expression;
                orderings.Add(ordering.Update(orderingExpression));
            }

            var offset = (SqlExpression)Visit(selectExpression.Offset);
            changed |= offset != selectExpression.Offset;

            var limit = (SqlExpression)Visit(selectExpression.Limit);
            changed |= limit != selectExpression.Limit;

            _canOptimize = canOptimize;

            // we assume SelectExpression can always be null
            // (e.g. projecting non-nullable column but with predicate that filters out all rows)
            Nullable = true;

            return changed
                ? selectExpression.Update(
                    projections, tables, predicate, groupBy, havingExpression, orderings, limit, offset, selectExpression.IsDistinct,
                    selectExpression.Alias)
                : selectExpression;
        }

        protected override Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
        {
            Nullable = false;
            var canOptimize = _canOptimize;

            // for SqlServer we could also allow optimize on children of ExpressionType.Equal
            // because they get converted to CASE blocks anyway, but for other providers it's incorrect
            // once/if null semantics optimizations are provider-specific we can enable it
            _canOptimize = _canOptimize && (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                || sqlBinaryExpression.OperatorType == ExpressionType.OrElse);

            var nonNullableColumns = new List<ColumnExpression>();
            if (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso)
            {
                nonNullableColumns = FindNonNullableColumns(sqlBinaryExpression.Left);
            }

            var newLeft = (SqlExpression)Visit(sqlBinaryExpression.Left);
            var leftNullable = Nullable;

            Nullable = false;
            if (nonNullableColumns.Count > 0)
            {
                _nonNullableColumns.AddRange(nonNullableColumns);
            }

            var newRight = (SqlExpression)Visit(sqlBinaryExpression.Right);
            var rightNullable = Nullable;

            foreach (var nonNullableColumn in nonNullableColumns)
            {
                _nonNullableColumns.Remove(nonNullableColumn);
            }

            if (sqlBinaryExpression.OperatorType == ExpressionType.Coalesce)
            {
                Nullable = leftNullable && rightNullable;
                _canOptimize = canOptimize;

                return sqlBinaryExpression.Update(newLeft, newRight);
            }

            if (sqlBinaryExpression.OperatorType == ExpressionType.Equal
                || sqlBinaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                ////var leftConstantNull = newLeft is SqlConstantExpression leftConstant && leftConstant.Value == null;
                ////var rightConstantNull = newRight is SqlConstantExpression rightConstant && rightConstant.Value == null;

                //var leftNull = (newLeft is SqlConstantExpression leftConstant && leftConstant.Value == null)
                //    || (newLeft is SqlParameterExpression leftParameter && _parametersValues[leftParameter.Name] == null);

                //var rightNull = (newRight is SqlConstantExpression rightConstant && rightConstant.Value == null)
                //    || (newRight is SqlParameterExpression rightParameter && _parametersValues[rightParameter.Name] == null);

                //// a == null -> a IS NULL
                //// a != null -> a IS NOT NULL
                //if (rightNull)
                //{
                //    Nullable = false;
                //    _canOptimize = canOptimize;

                //    return sqlBinaryExpression.OperatorType == ExpressionType.Equal
                //        ? _sqlExpressionFactory.IsNull(newLeft)
                //        : _sqlExpressionFactory.IsNotNull(newLeft);
                //}

                //// null == a -> a IS NULL
                //// null != a -> a IS NOT NULL
                //if (leftNull)
                //{
                //    Nullable = false;
                //    _canOptimize = canOptimize;

                //    return sqlBinaryExpression.OperatorType == ExpressionType.Equal
                //        ? _sqlExpressionFactory.IsNull(newRight)
                //        : _sqlExpressionFactory.IsNotNull(newRight);
                //}

                if (_useRelationalNulls)
                {
                    Nullable = leftNullable || rightNullable;

                    return sqlBinaryExpression.Update(newLeft, newRight);
                }

                var leftUnary = newLeft as SqlUnaryExpression;
                var rightUnary = newRight as SqlUnaryExpression;

                var leftNegated = leftUnary?.IsLogicalNot() == true;
                var rightNegated = rightUnary?.IsLogicalNot() == true;

                if (leftNegated)
                {
                    newLeft = leftUnary.Operand;
                }

                if (rightNegated)
                {
                    newRight = rightUnary.Operand;
                }

                var leftIsNull = _sqlExpressionFactory.IsNull(newLeft);
                var rightIsNull = _sqlExpressionFactory.IsNull(newRight);

                // optimized expansion which doesn't distinguish between null and false
                if (canOptimize
                    && sqlBinaryExpression.OperatorType == ExpressionType.Equal
                    && !leftNegated
                    && !rightNegated)
                {
                    // when we use optimized form, the result can still be nullable
                    if (leftNullable && rightNullable)
                    {
                        Nullable = true;
                        _canOptimize = canOptimize;

                        return _sqlExpressionFactory.OrElse(
                            _sqlExpressionFactory.Equal(newLeft, newRight),
                            _sqlExpressionFactory.AndAlso(leftIsNull, rightIsNull));
                    }

                    if ((leftNullable && !rightNullable)
                        || (!leftNullable && rightNullable))
                    {
                        Nullable = true;
                        _canOptimize = canOptimize;

                        return _sqlExpressionFactory.Equal(newLeft, newRight);
                    }
                }

                // doing a full null semantics rewrite - removing all nulls from truth table
                // this will NOT be correct once we introduce simplified null semantics
                Nullable = false;
                _canOptimize = canOptimize;

                if (sqlBinaryExpression.OperatorType == ExpressionType.Equal)
                {
                    if (!leftNullable
                        && !rightNullable)
                    {
                        // a == b <=> !a == !b -> a == b
                        // !a == b <=> a == !b -> a != b
                        return leftNegated == rightNegated
                            ? _sqlExpressionFactory.Equal(newLeft, newRight)
                            : _sqlExpressionFactory.NotEqual(newLeft, newRight);
                    }

                    if (leftNullable && rightNullable)
                    {
                        // ?a == ?b <=> !(?a) == !(?b) -> [(a == b) && (a != null && b != null)] || (a == null && b == null))
                        // !(?a) == ?b <=> ?a == !(?b) -> [(a != b) && (a != null && b != null)] || (a == null && b == null)
                        return leftNegated == rightNegated
                            ? ExpandNullableEqualNullable(newLeft, newRight, leftIsNull, rightIsNull)
                            : ExpandNegatedNullableEqualNullable(newLeft, newRight, leftIsNull, rightIsNull);
                    }

                    if (leftNullable && !rightNullable)
                    {
                        // ?a == b <=> !(?a) == !b -> (a == b) && (a != null)
                        // !(?a) == b <=> ?a == !b -> (a != b) && (a != null)
                        return leftNegated == rightNegated
                            ? ExpandNullableEqualNonNullable(newLeft, newRight, leftIsNull)
                            : ExpandNegatedNullableEqualNonNullable(newLeft, newRight, leftIsNull);
                    }

                    if (rightNullable && !leftNullable)
                    {
                        // a == ?b <=> !a == !(?b) -> (a == b) && (b != null)
                        // !a == ?b <=> a == !(?b) -> (a != b) && (b != null)
                        return leftNegated == rightNegated
                            ? ExpandNullableEqualNonNullable(newLeft, newRight, rightIsNull)
                            : ExpandNegatedNullableEqualNonNullable(newLeft, newRight, rightIsNull);
                    }
                }

                if (sqlBinaryExpression.OperatorType == ExpressionType.NotEqual)
                {
                    if (!leftNullable
                        && !rightNullable)
                    {
                        // a != b <=> !a != !b -> a != b
                        // !a != b <=> a != !b -> a == b
                        return leftNegated == rightNegated
                            ? _sqlExpressionFactory.NotEqual(newLeft, newRight)
                            : _sqlExpressionFactory.Equal(newLeft, newRight);
                    }

                    if (leftNullable && rightNullable)
                    {
                        // ?a != ?b <=> !(?a) != !(?b) -> [(a != b) || (a == null || b == null)] && (a != null || b != null)
                        // !(?a) != ?b <=> ?a != !(?b) -> [(a == b) || (a == null || b == null)] && (a != null || b != null)
                        return leftNegated == rightNegated
                            ? ExpandNullableNotEqualNullable(newLeft, newRight, leftIsNull, rightIsNull)
                            : ExpandNegatedNullableNotEqualNullable(newLeft, newRight, leftIsNull, rightIsNull);
                    }

                    if (leftNullable)
                    {
                        // ?a != b <=> !(?a) != !b -> (a != b) || (a == null)
                        // !(?a) != b <=> ?a != !b -> (a == b) || (a == null)
                        return leftNegated == rightNegated
                            ? ExpandNullableNotEqualNonNullable(newLeft, newRight, leftIsNull)
                            : ExpandNegatedNullableNotEqualNonNullable(newLeft, newRight, leftIsNull);
                    }

                    if (rightNullable)
                    {
                        // a != ?b <=> !a != !(?b) -> (a != b) || (b == null)
                        // !a != ?b <=> a != !(?b) -> (a == b) || (b == null)
                        return leftNegated == rightNegated
                            ? ExpandNullableNotEqualNonNullable(newLeft, newRight, rightIsNull)
                            : ExpandNegatedNullableNotEqualNonNullable(newLeft, newRight, rightIsNull);
                    }
                }
            }

            Nullable = leftNullable || rightNullable;
            _canOptimize = canOptimize;

            return sqlBinaryExpression.Update(newLeft, newRight);
        }

        //protected override SqlExpression SimplifyBinaryExpression2(SqlBinaryExpression sqlBinaryExpression)
        //{
        //    switch (sqlBinaryExpression.OperatorType)
        //    {
        //        case ExpressionType.Equal:
        //        case ExpressionType.NotEqual:
        //            var leftNullParameter = sqlBinaryExpression.Left is SqlParameterExpression leftParameter && _parametersValues[leftParameter.Name] == null;
        //            var rightNullParameter = sqlBinaryExpression.Right is SqlParameterExpression rightParameter && _parametersValues[rightParameter.Name] == null;
        //            if (leftNullParameter || rightNullParameter)
        //            {
        //                return SimplifyNullComparisonExpression(
        //                    sqlBinaryExpression.OperatorType,
        //                    sqlBinaryExpression.Left,
        //                    sqlBinaryExpression.Right,
        //                    leftNullParameter,
        //                    rightNullParameter,
        //                    sqlBinaryExpression.TypeMapping);
        //            }

        //            break;
        //    }

        //    return base.SimplifyBinaryExpression2(sqlBinaryExpression);
        //}

        //protected override SqlExpression SimplifyBinaryExpression(
        //    ExpressionType operatorType,
        //    SqlExpression left,
        //    SqlExpression right,
        //    RelationalTypeMapping typeMapping)
        //{
        //    switch (operatorType)
        //    {
        //        case ExpressionType.Equal:
        //        case ExpressionType.NotEqual:
        //            var leftNullParameter = left is SqlParameterExpression leftParameter && _parametersValues[leftParameter.Name] == null;
        //            var rightNullParameter = right is SqlParameterExpression rightParameter && _parametersValues[rightParameter.Name] == null;
        //            if (leftNullParameter || rightNullParameter)
        //            {
        //                return SimplifyNullComparisonExpression(
        //                    operatorType,
        //                    left,
        //                    right,
        //                    leftNullParameter,
        //                    rightNullParameter,
        //                    typeMapping);
        //            }

        //            break;
        //    }

        //    return base.SimplifyBinaryExpression(operatorType, left, right, typeMapping);
        //}


        protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
            => CantOptimize(base.VisitSqlFunction, sqlFunctionExpression);

        protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
        {
            Nullable = _parametersValues[sqlParameterExpression.Name] == null;

            return sqlParameterExpression;
        }

        protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
        {
            Nullable = false;

            var canOptimize = _canOptimize;
            _canOptimize = false;

            var operand = (SqlExpression)Visit(sqlUnaryExpression.Operand);

            // result of IsNull/IsNotNull can never be null
            if (sqlUnaryExpression.OperatorType == ExpressionType.Equal
                || sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                Nullable = false;

                if (operand is SqlParameterExpression parameterOperand)
                {
                    var parameterValue = _parametersValues[parameterOperand.Name];
                    if (sqlUnaryExpression.OperatorType == ExpressionType.Equal)
                    {
                        _canOptimize = canOptimize;

                        return _sqlExpressionFactory.Constant(parameterValue == null, sqlUnaryExpression.TypeMapping);
                    }

                    if (sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
                    {
                        _canOptimize = canOptimize;

                        return _sqlExpressionFactory.Constant(parameterValue != null, sqlUnaryExpression.TypeMapping);
                    }
                }
            }

            _canOptimize = canOptimize;

            return sqlUnaryExpression.Update(operand);
        }

        //protected override SqlExpression SimplifyUnaryExpression(
        //    ExpressionType operatorType,
        //    SqlExpression operand,
        //    Type type,
        //    RelationalTypeMapping typeMapping)
        //{
        //    if (operatorType == ExpressionType.Not
        //        && _useRelationalNulls
        //        && (type == typeof(bool) || type == typeof(bool?))
        //        && operand is SqlBinaryExpression binaryOperand)
        //    {
        //        // De Morgan's
        //        if (binaryOperand.OperatorType == ExpressionType.AndAlso
        //            || binaryOperand.OperatorType == ExpressionType.OrElse)
        //        {
        //            var newLeft = SimplifyUnaryExpression(ExpressionType.Not, binaryOperand.Left, type, typeMapping);
        //            var newRight = SimplifyUnaryExpression(ExpressionType.Not, binaryOperand.Right, type, typeMapping);

        //            return SimplifyBinaryExpression2(//SimplifyLogicalSqlBinaryExpression(
        //                SqlExpressionFactory.MakeBinary(
        //                    binaryOperand.OperatorType == ExpressionType.AndAlso
        //                        ? ExpressionType.OrElse
        //                        : ExpressionType.AndAlso,
        //                newLeft,
        //                newRight,
        //                binaryOperand.TypeMapping));

        //            //return SimplifyBinaryExpression(//SimplifyLogicalSqlBinaryExpression(
        //            //    binaryOperand.OperatorType == ExpressionType.AndAlso
        //            //        ? ExpressionType.OrElse
        //            //        : ExpressionType.AndAlso,
        //            //    newLeft,
        //            //    newRight,
        //            //    binaryOperand.TypeMapping);
        //        }
        //    }

        //    //switch (operatorType)
        //    //{
        //    //    case ExpressionType.Not
        //    //        when _useRelationalNulls && (type == typeof(bool) || type == typeof(bool?)) && operand is SqlBinaryExpression binaryOperand:
        //    //    {
        //    //        // De Morgan's
        //    //        if (binaryOperand.OperatorType == ExpressionType.AndAlso
        //    //            || binaryOperand.OperatorType == ExpressionType.OrElse)
        //    //        {
        //    //            var newLeft = SimplifyUnaryExpression(ExpressionType.Not, binaryOperand.Left, type, typeMapping);
        //    //            var newRight = SimplifyUnaryExpression(ExpressionType.Not, binaryOperand.Right, type, typeMapping);

        //    //            return SimplifyLogicalSqlBinaryExpression(
        //    //                binaryOperand.OperatorType == ExpressionType.AndAlso
        //    //                    ? ExpressionType.OrElse
        //    //                    : ExpressionType.AndAlso,
        //    //                newLeft,
        //    //                newRight,
        //    //                binaryOperand.TypeMapping);
        //    //        }

        //    //        //switch (operand)
        //    //        //{
        //    //        //        //case SqlBinaryExpression binaryOperand:
        //    //        //        //{

        //    //        //        //    //// those optimizations are only valid in 2-value logic
        //    //        //        //    //// they are safe to do here because if we apply null semantics
        //    //        //        //    //// because null semantics removes possibility of nulls in the tree when the comparison is wrapped around NOT
        //    //        //        //    //if (!_useRelationalNulls
        //    //        //        //    //    && TryNegate(binaryOperand.OperatorType, out var negated))
        //    //        //        //    //{
        //    //        //        //    //    return SimplifyBinaryExpression(
        //    //        //        //    //        negated,
        //    //        //        //    //        binaryOperand.Left,
        //    //        //        //    //        binaryOperand.Right,
        //    //        //        //    //        binaryOperand.TypeMapping);
        //    //        //        //    //}
        //    //        //        //}
        //    //        //        //break;
        //    //        //}
        //    //        break;
        //    //    }
        //    //}

        //    return base.SimplifyUnaryExpression(operatorType, operand, type, typeMapping);
        //}

        protected override Expression VisitUnion(UnionExpression unionExpression)
            => CantOptimize(base.VisitUnion, unionExpression);

        private Expression CantOptimize<TExpression>(Func<TExpression, Expression> method, TExpression expression)
            where TExpression : Expression
        {
            var canOptimize = _canOptimize;
            _canOptimize = false;
            var result = method(expression);
            _canOptimize = canOptimize;

            return result;
        }

        private List<ColumnExpression> FindNonNullableColumns(SqlExpression sqlExpression)
        {
            var result = new List<ColumnExpression>();
            if (sqlExpression is SqlBinaryExpression sqlBinaryExpression)
            {
                if (sqlBinaryExpression.OperatorType == ExpressionType.NotEqual)
                {
                    if (sqlBinaryExpression.Left is ColumnExpression leftColumn
                        && leftColumn.IsNullable
                        && sqlBinaryExpression.Right is SqlConstantExpression rightConstant
                        && rightConstant.Value == null)
                    {
                        result.Add(leftColumn);
                    }

                    if (sqlBinaryExpression.Right is ColumnExpression rightColumn
                        && rightColumn.IsNullable
                        && sqlBinaryExpression.Left is SqlConstantExpression leftConstant
                        && leftConstant.Value == null)
                    {
                        result.Add(rightColumn);
                    }
                }

                if (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso)
                {
                    result.AddRange(FindNonNullableColumns(sqlBinaryExpression.Left));
                    result.AddRange(FindNonNullableColumns(sqlBinaryExpression.Right));
                }
            }

            return result;
        }

        // ?a == ?b -> [(a == b) && (a != null && b != null)] || (a == null && b == null))
        //
        // a | b | F1 = a == b | F2 = (a != null && b != null) | F3 = F1 && F2 |
        //   |   |             |                               |               |
        // 0 | 0 | 1           | 1                             | 1             |
        // 0 | 1 | 0           | 1                             | 0             |
        // 0 | N | N           | 0                             | 0             |
        // 1 | 0 | 0           | 1                             | 0             |
        // 1 | 1 | 1           | 1                             | 1             |
        // 1 | N | N           | 0                             | 0             |
        // N | 0 | N           | 0                             | 0             |
        // N | 1 | N           | 0                             | 0             |
        // N | N | N           | 0                             | 0             |
        //
        // a | b | F4 = (a == null && b == null) | Final = F3 OR F4 |
        //   |   |                               |                  |
        // 0 | 0 | 0                             | 1 OR 0 = 1       |
        // 0 | 1 | 0                             | 0 OR 0 = 0       |
        // 0 | N | 0                             | 0 OR 0 = 0       |
        // 1 | 0 | 0                             | 0 OR 0 = 0       |
        // 1 | 1 | 0                             | 1 OR 0 = 1       |
        // 1 | N | 0                             | 0 OR 0 = 0       |
        // N | 0 | 0                             | 0 OR 0 = 0       |
        // N | 1 | 0                             | 0 OR 0 = 0       |
        // N | N | 1                             | 0 OR 1 = 1       |
        private SqlBinaryExpression ExpandNullableEqualNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull, SqlExpression rightIsNull)
            => _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.AndAlso(
                    _sqlExpressionFactory.Equal(left, right),
                    _sqlExpressionFactory.AndAlso(
                        _sqlExpressionFactory.Not(leftIsNull),
                        _sqlExpressionFactory.Not(rightIsNull))),
                _sqlExpressionFactory.AndAlso(
                    leftIsNull,
                    rightIsNull));

        // !(?a) == ?b -> [(a != b) && (a != null && b != null)] || (a == null && b == null)
        //
        // a | b | F1 = a != b | F2 = (a != null && b != null) | F3 = F1 && F2 |
        //   |   |             |                               |               |
        // 0 | 0 | 0           | 1                             | 0             |
        // 0 | 1 | 1           | 1                             | 1             |
        // 0 | N | N           | 0                             | 0             |
        // 1 | 0 | 1           | 1                             | 1             |
        // 1 | 1 | 0           | 1                             | 0             |
        // 1 | N | N           | 0                             | 0             |
        // N | 0 | N           | 0                             | 0             |
        // N | 1 | N           | 0                             | 0             |
        // N | N | N           | 0                             | 0             |
        //
        // a | b | F4 = (a == null && b == null) | Final = F3 OR F4 |
        //   |   |                               |                  |
        // 0 | 0 | 0                             | 0 OR 0 = 0       |
        // 0 | 1 | 0                             | 1 OR 0 = 1       |
        // 0 | N | 0                             | 0 OR 0 = 0       |
        // 1 | 0 | 0                             | 1 OR 0 = 1       |
        // 1 | 1 | 0                             | 0 OR 0 = 0       |
        // 1 | N | 0                             | 0 OR 0 = 0       |
        // N | 0 | 0                             | 0 OR 0 = 0       |
        // N | 1 | 0                             | 0 OR 0 = 0       |
        // N | N | 1                             | 0 OR 1 = 1       |
        private SqlBinaryExpression ExpandNegatedNullableEqualNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull, SqlExpression rightIsNull)
            => _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.AndAlso(
                    _sqlExpressionFactory.NotEqual(left, right),
                    _sqlExpressionFactory.AndAlso(
                        _sqlExpressionFactory.Not(leftIsNull),
                        _sqlExpressionFactory.Not(rightIsNull))),
                _sqlExpressionFactory.AndAlso(
                    leftIsNull,
                    rightIsNull));

        // ?a == b -> (a == b) && (a != null)
        //
        // a | b | F1 = a == b | F2 = (a != null) | Final = F1 && F2 |
        //   |   |             |                  |                  |
        // 0 | 0 | 1           | 1                | 1                |
        // 0 | 1 | 0           | 1                | 0                |
        // 1 | 0 | 0           | 1                | 0                |
        // 1 | 1 | 1           | 1                | 1                |
        // N | 0 | N           | 0                | 0                |
        // N | 1 | N           | 0                | 0                |
        private SqlBinaryExpression ExpandNullableEqualNonNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull)
            => _sqlExpressionFactory.AndAlso(
                _sqlExpressionFactory.Equal(left, right),
                _sqlExpressionFactory.Not(leftIsNull));

        // !(?a) == b -> (a != b) && (a != null)
        //
        // a | b | F1 = a != b | F2 = (a != null) | Final = F1 && F2 |
        //   |   |             |                  |                  |
        // 0 | 0 | 0           | 1                | 0                |
        // 0 | 1 | 1           | 1                | 1                |
        // 1 | 0 | 1           | 1                | 1                |
        // 1 | 1 | 0           | 1                | 0                |
        // N | 0 | N           | 0                | 0                |
        // N | 1 | N           | 0                | 0                |
        private SqlBinaryExpression ExpandNegatedNullableEqualNonNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull)
            => _sqlExpressionFactory.AndAlso(
                _sqlExpressionFactory.NotEqual(left, right),
                _sqlExpressionFactory.Not(leftIsNull));

        // ?a != ?b -> [(a != b) || (a == null || b == null)] && (a != null || b != null)
        //
        // a | b | F1 = a != b | F2 = (a == null || b == null) | F3 = F1 || F2 |
        //   |   |             |                               |               |
        // 0 | 0 | 0           | 0                             | 0             |
        // 0 | 1 | 1           | 0                             | 1             |
        // 0 | N | N           | 1                             | 1             |
        // 1 | 0 | 1           | 0                             | 1             |
        // 1 | 1 | 0           | 0                             | 0             |
        // 1 | N | N           | 1                             | 1             |
        // N | 0 | N           | 1                             | 1             |
        // N | 1 | N           | 1                             | 1             |
        // N | N | N           | 1                             | 1             |
        //
        // a | b | F4 = (a != null || b != null) | Final = F3 && F4 |
        //   |   |                               |                  |
        // 0 | 0 | 1                             | 0 && 1 = 0       |
        // 0 | 1 | 1                             | 1 && 1 = 1       |
        // 0 | N | 1                             | 1 && 1 = 1       |
        // 1 | 0 | 1                             | 1 && 1 = 1       |
        // 1 | 1 | 1                             | 0 && 1 = 0       |
        // 1 | N | 1                             | 1 && 1 = 1       |
        // N | 0 | 1                             | 1 && 1 = 1       |
        // N | 1 | 1                             | 1 && 1 = 1       |
        // N | N | 0                             | 1 && 0 = 0       |
        private SqlBinaryExpression ExpandNullableNotEqualNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull, SqlExpression rightIsNull)
            => _sqlExpressionFactory.AndAlso(
                _sqlExpressionFactory.OrElse(
                    _sqlExpressionFactory.NotEqual(left, right),
                    _sqlExpressionFactory.OrElse(
                        leftIsNull,
                        rightIsNull)),
                _sqlExpressionFactory.OrElse(
                    _sqlExpressionFactory.Not(leftIsNull),
                    _sqlExpressionFactory.Not(rightIsNull)));

        // !(?a) != ?b -> [(a == b) || (a == null || b == null)] && (a != null || b != null)
        //
        // a | b | F1 = a == b | F2 = (a == null || b == null) | F3 = F1 || F2 |
        //   |   |             |                               |               |
        // 0 | 0 | 1           | 0                             | 1             |
        // 0 | 1 | 0           | 0                             | 0             |
        // 0 | N | N           | 1                             | 1             |
        // 1 | 0 | 0           | 0                             | 0             |
        // 1 | 1 | 1           | 0                             | 1             |
        // 1 | N | N           | 1                             | 1             |
        // N | 0 | N           | 1                             | 1             |
        // N | 1 | N           | 1                             | 1             |
        // N | N | N           | 1                             | 1             |
        //
        // a | b | F4 = (a != null || b != null) | Final = F3 && F4 |
        //   |   |                               |                  |
        // 0 | 0 | 1                             | 1 && 1 = 1       |
        // 0 | 1 | 1                             | 0 && 1 = 0       |
        // 0 | N | 1                             | 1 && 1 = 1       |
        // 1 | 0 | 1                             | 0 && 1 = 0       |
        // 1 | 1 | 1                             | 1 && 1 = 1       |
        // 1 | N | 1                             | 1 && 1 = 1       |
        // N | 0 | 1                             | 1 && 1 = 1       |
        // N | 1 | 1                             | 1 && 1 = 1       |
        // N | N | 0                             | 1 && 0 = 0       |
        private SqlBinaryExpression ExpandNegatedNullableNotEqualNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull, SqlExpression rightIsNull)
            => _sqlExpressionFactory.AndAlso(
                _sqlExpressionFactory.OrElse(
                    _sqlExpressionFactory.Equal(left, right),
                    _sqlExpressionFactory.OrElse(
                        leftIsNull,
                        rightIsNull)),
                _sqlExpressionFactory.OrElse(
                    _sqlExpressionFactory.Not(leftIsNull),
                    _sqlExpressionFactory.Not(rightIsNull)));

        // ?a != b -> (a != b) || (a == null)
        //
        // a | b | F1 = a != b | F2 = (a == null) | Final = F1 OR F2 |
        //   |   |             |                  |                  |
        // 0 | 0 | 0           | 0                | 0                |
        // 0 | 1 | 1           | 0                | 1                |
        // 1 | 0 | 1           | 0                | 1                |
        // 1 | 1 | 0           | 0                | 0                |
        // N | 0 | N           | 1                | 1                |
        // N | 1 | N           | 1                | 1                |
        private SqlBinaryExpression ExpandNullableNotEqualNonNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull)
            => _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.NotEqual(left, right),
                leftIsNull);

        // !(?a) != b -> (a == b) || (a == null)
        //
        // a | b | F1 = a == b | F2 = (a == null) | F3 = F1 OR F2 |
        //   |   |             |                  |               |
        // 0 | 0 | 1           | 0                | 1             |
        // 0 | 1 | 0           | 0                | 0             |
        // 1 | 0 | 0           | 0                | 0             |
        // 1 | 1 | 1           | 0                | 1             |
        // N | 0 | N           | 1                | 1             |
        // N | 1 | N           | 1                | 1             |
        private SqlBinaryExpression ExpandNegatedNullableNotEqualNonNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull)
            => _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.Equal(left, right),
                leftIsNull);
    }
}
