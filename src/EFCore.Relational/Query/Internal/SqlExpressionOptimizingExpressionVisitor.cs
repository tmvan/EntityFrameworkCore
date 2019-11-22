// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public class SqlExpressionOptimizingExpressionVisitor : SqlExpressionVisitor
    {
        //private static bool TryNegate(ExpressionType expressionType, out ExpressionType result)
        //{
        //    var negated = expressionType switch
        //    {
        //        ExpressionType.AndAlso => ExpressionType.OrElse,
        //        ExpressionType.OrElse => ExpressionType.AndAlso,
        //        ExpressionType.Equal => ExpressionType.NotEqual,
        //        ExpressionType.NotEqual => ExpressionType.Equal,
        //        ExpressionType.GreaterThan => ExpressionType.LessThanOrEqual,
        //        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThan,
        //        ExpressionType.LessThan => ExpressionType.GreaterThanOrEqual,
        //        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThan,
        //        _ => (ExpressionType?)null
        //    };

        //    result = negated ?? default;

        //    return negated.HasValue;
        //}

        public SqlExpressionOptimizingExpressionVisitor(ISqlExpressionFactory sqlExpressionFactory)
        {
            SqlExpressionFactory = sqlExpressionFactory;
        }

        protected virtual ISqlExpressionFactory SqlExpressionFactory { get; }

        protected override Expression VisitCase(CaseExpression caseExpression)
        {
            var newOperand = (SqlExpression)Visit(caseExpression.Operand);
            var newWhenClauses = new List<CaseWhenClause>();
            foreach (var whenClause in caseExpression.WhenClauses)
            {
                var newTest = (SqlExpression)Visit(whenClause.Test);
                var newResult = (SqlExpression)Visit(whenClause.Result);
                newWhenClauses.Add(new CaseWhenClause(newTest, newResult));
            }

            var newElseResult = (SqlExpression)Visit(caseExpression.ElseResult);

            return caseExpression.Update(newOperand, newWhenClauses, newElseResult);
        }

        protected override Expression VisitColumn(ColumnExpression columnExpression)
            => columnExpression;

        protected override Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
        {
            var table = (TableExpressionBase)Visit(crossApplyExpression.Table);

            return crossApplyExpression.Update(table);
        }

        protected override Expression VisitCrossJoin(CrossJoinExpression crossJoinExpression)
        {
            var table = (TableExpressionBase)Visit(crossJoinExpression.Table);

            return crossJoinExpression.Update(table);
        }

        protected override Expression VisitExcept(ExceptExpression exceptExpression)
        {
            var source1 = (SelectExpression)Visit(exceptExpression.Source1);
            var source2 = (SelectExpression)Visit(exceptExpression.Source2);

            return exceptExpression.Update(source1, source2);
        }

        protected override Expression VisitExists(ExistsExpression existsExpression)
        {
            var newSubquery = (SelectExpression)Visit(existsExpression.Subquery);

            return existsExpression.Update(newSubquery);
        }

        protected override Expression VisitFromSql(FromSqlExpression fromSqlExpression)
            => fromSqlExpression;

        protected override Expression VisitIn(InExpression inExpression)
        {
            var item = (SqlExpression)Visit(inExpression.Item);
            var subquery = (SelectExpression)Visit(inExpression.Subquery);
            var values = (SqlExpression)Visit(inExpression.Values);

            return inExpression.Update(item, values, subquery);
        }

        protected override Expression VisitIntersect(IntersectExpression intersectExpression)
        {
            var source1 = (SelectExpression)Visit(intersectExpression.Source1);
            var source2 = (SelectExpression)Visit(intersectExpression.Source2);

            return intersectExpression.Update(source1, source2);
        }

        protected override Expression VisitLike(LikeExpression likeExpression)
        {
            var newMatch = (SqlExpression)Visit(likeExpression.Match);
            var newPattern = (SqlExpression)Visit(likeExpression.Pattern);
            var newEscapeChar = (SqlExpression)Visit(likeExpression.EscapeChar);

            return likeExpression.Update(newMatch, newPattern, newEscapeChar);
        }

        protected override Expression VisitInnerJoin(InnerJoinExpression innerJoinExpression)
        {
            var newTable = (TableExpressionBase)Visit(innerJoinExpression.Table);
            var newJoinPredicate = (SqlExpression)Visit(innerJoinExpression.JoinPredicate);

            return innerJoinExpression.Update(newTable, newJoinPredicate);
        }

        protected override Expression VisitLeftJoin(LeftJoinExpression leftJoinExpression)
        {
            var newTable = (TableExpressionBase)Visit(leftJoinExpression.Table);
            var newJoinPredicate = (SqlExpression)Visit(leftJoinExpression.JoinPredicate);

            return leftJoinExpression.Update(newTable, newJoinPredicate);
        }

        protected override Expression VisitOrdering(OrderingExpression orderingExpression)
        {
            var newExpression = (SqlExpression)Visit(orderingExpression.Expression);

            return orderingExpression.Update(newExpression);
        }

        protected override Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
        {
            var table = (TableExpressionBase)Visit(outerApplyExpression.Table);

            return outerApplyExpression.Update(table);
        }

        protected override Expression VisitProjection(ProjectionExpression projectionExpression)
        {
            var expression = (SqlExpression)Visit(projectionExpression.Expression);

            return projectionExpression.Update(expression);
        }

        protected override Expression VisitRowNumber(RowNumberExpression rowNumberExpression)
        {
            var partitions = new List<SqlExpression>();
            foreach (var partition in rowNumberExpression.Partitions)
            {
                var newPartition = (SqlExpression)Visit(partition);
                partitions.Add(newPartition);
            }

            var orderings = new List<OrderingExpression>();
            foreach (var ordering in rowNumberExpression.Orderings)
            {
                var newOrdering = (OrderingExpression)Visit(ordering);
                orderings.Add(newOrdering);
            }

            return rowNumberExpression.Update(partitions, orderings);
        }

        protected override Expression VisitScalarSubquery(ScalarSubqueryExpression scalarSubqueryExpression)
        {
            var subquery = (SelectExpression)Visit(scalarSubqueryExpression.Subquery);

            return scalarSubqueryExpression.Update(subquery);
        }

        protected override Expression VisitSelect(SelectExpression selectExpression)
        {
            var changed = false;
            var projections = new List<ProjectionExpression>();
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

            var predicate = (SqlExpression)Visit(selectExpression.Predicate);
            changed |= predicate != selectExpression.Predicate;
            if (predicate is SqlConstantExpression predicateConstantExpression
                && predicateConstantExpression.Value is bool predicateBoolValue
                && predicateBoolValue)
            {
                predicate = null;
                changed = true;
            }

            var groupBy = new List<SqlExpression>();
            foreach (var groupingKey in selectExpression.GroupBy)
            {
                var newGroupingKey = (SqlExpression)Visit(groupingKey);
                changed |= newGroupingKey != groupingKey;
                groupBy.Add(newGroupingKey);
            }

            var having = (SqlExpression)Visit(selectExpression.Having);
            changed |= having != selectExpression.Having;
            if (having is SqlConstantExpression havingConstantExpression
                && havingConstantExpression.Value is bool havingBoolValue
                && havingBoolValue)
            {
                having = null;
                changed = true;
            }

            var orderings = new List<OrderingExpression>();
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

            return changed
                ? selectExpression.Update(
                    projections, tables, predicate, groupBy, having, orderings, limit, offset, selectExpression.IsDistinct,
                    selectExpression.Alias)
                : selectExpression;
        }

        protected override Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
        {
            var newLeft = (SqlExpression)Visit(sqlBinaryExpression.Left);
            var newRight = (SqlExpression)Visit(sqlBinaryExpression.Right);

            return SimplifyBinaryExpression2(sqlBinaryExpression.Update(newLeft, newRight));

            //return SimplifyBinaryExpression(
            //    sqlBinaryExpression.OperatorType,
            //    newLeft,
            //    newRight,
            //    sqlBinaryExpression.TypeMapping);
        }

        protected virtual SqlExpression SimplifyBinaryExpression2(SqlBinaryExpression sqlBinaryExpression)
        {
            switch (sqlBinaryExpression.OperatorType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                    var leftUnary = sqlBinaryExpression.Left as SqlUnaryExpression;
                    var rightUnary = sqlBinaryExpression.Right as SqlUnaryExpression;
                    if (leftUnary != null
                        && rightUnary != null
                        && (leftUnary.OperatorType == ExpressionType.Equal || leftUnary.OperatorType == ExpressionType.NotEqual)
                        && (rightUnary.OperatorType == ExpressionType.Equal || rightUnary.OperatorType == ExpressionType.NotEqual)
                        && leftUnary.Operand.Equals(rightUnary.Operand))
                    {
                        // a is null || a is null -> a is null
                        // a is not null || a is not null -> a is not null
                        // a is null && a is null -> a is null
                        // a is not null && a is not null -> a is not null
                        // a is null || a is not null -> true
                        // a is null && a is not null -> false
                        return leftUnary.OperatorType == rightUnary.OperatorType
                            ? (SqlExpression)leftUnary
                            : SqlExpressionFactory.Constant(sqlBinaryExpression.OperatorType == ExpressionType.OrElse, sqlBinaryExpression.TypeMapping);
                    }

                    return SimplifyLogicalSqlBinaryExpression(
                        sqlBinaryExpression.OperatorType,
                        sqlBinaryExpression.Left,
                        sqlBinaryExpression.Right,
                        sqlBinaryExpression.TypeMapping);

                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                    var leftConstant = sqlBinaryExpression.Left as SqlConstantExpression;
                    var rightConstant = sqlBinaryExpression.Right as SqlConstantExpression;
                    var leftNullConstant = leftConstant != null && leftConstant.Value == null;
                    var rightNullConstant = rightConstant != null && rightConstant.Value == null;
                    if (leftNullConstant || rightNullConstant)
                    {
                        return SimplifyNullComparisonExpression(
                            sqlBinaryExpression.OperatorType,
                            sqlBinaryExpression.Left,
                            sqlBinaryExpression.Right,
                            leftNullConstant,
                            rightNullConstant,
                            sqlBinaryExpression.TypeMapping);
                    }

                    var leftBoolConstant = sqlBinaryExpression.Left.Type == typeof(bool) ? leftConstant : null;
                    var rightBoolConstant = sqlBinaryExpression.Right.Type == typeof(bool) ? rightConstant : null;
                    if (leftBoolConstant != null || rightBoolConstant != null)
                    {
                        return SimplifyBoolConstantComparisonExpression(
                            sqlBinaryExpression.OperatorType,
                            sqlBinaryExpression.Left,
                            sqlBinaryExpression.Right,
                            leftBoolConstant,
                            rightBoolConstant,
                            sqlBinaryExpression.TypeMapping);
                    }

                    // only works when a is not nullable
                    // a == a -> true
                    // a != a -> false
                    if ((sqlBinaryExpression.Left is LikeExpression
                        || sqlBinaryExpression.Left is ColumnExpression columnExpression && !columnExpression.IsNullable)
                        && sqlBinaryExpression.Left.Equals(sqlBinaryExpression.Right))
                    {
                        return SqlExpressionFactory.Constant(sqlBinaryExpression.OperatorType == ExpressionType.Equal, sqlBinaryExpression.TypeMapping);
                    }

                    break;
            }

            return sqlBinaryExpression;// SqlExpressionFactory.MakeBinary(operatorType, left, right, typeMapping);
        }

        //protected virtual SqlExpression SimplifyBinaryExpression(
        //    ExpressionType operatorType,
        //    SqlExpression left,
        //    SqlExpression right,
        //    RelationalTypeMapping typeMapping)
        //{
        //    switch (operatorType)
        //    {
        //        case ExpressionType.AndAlso:
        //        case ExpressionType.OrElse:
        //            var leftUnary = left as SqlUnaryExpression;
        //            var rightUnary = right as SqlUnaryExpression;
        //            if (leftUnary != null
        //                && rightUnary != null
        //                && (leftUnary.OperatorType == ExpressionType.Equal || leftUnary.OperatorType == ExpressionType.NotEqual)
        //                && (rightUnary.OperatorType == ExpressionType.Equal || rightUnary.OperatorType == ExpressionType.NotEqual)
        //                && leftUnary.Operand.Equals(rightUnary.Operand))
        //            {
        //                // a is null || a is null -> a is null
        //                // a is not null || a is not null -> a is not null
        //                // a is null && a is null -> a is null
        //                // a is not null && a is not null -> a is not null
        //                // a is null || a is not null -> true
        //                // a is null && a is not null -> false
        //                return leftUnary.OperatorType == rightUnary.OperatorType
        //                    ? (SqlExpression)leftUnary
        //                    : SqlExpressionFactory.Constant(operatorType == ExpressionType.OrElse, typeMapping);
        //            }

        //            return SimplifyLogicalSqlBinaryExpression(
        //                operatorType,
        //                left,
        //                right,
        //                typeMapping);

        //        case ExpressionType.Equal:
        //        case ExpressionType.NotEqual:
        //            var leftConstant = left as SqlConstantExpression;
        //            var rightConstant = right as SqlConstantExpression;
        //            var leftNullConstant = leftConstant != null && leftConstant.Value == null;
        //            var rightNullConstant = rightConstant != null && rightConstant.Value == null;
        //            if (leftNullConstant || rightNullConstant)
        //            {
        //                return SimplifyNullComparisonExpression(
        //                    operatorType,
        //                    left,
        //                    right,
        //                    leftNullConstant,
        //                    rightNullConstant,
        //                    typeMapping);
        //            }

        //            var leftBoolConstant = left.Type == typeof(bool) ? leftConstant : null;
        //            var rightBoolConstant = right.Type == typeof(bool) ? rightConstant : null;
        //            if (leftBoolConstant != null || rightBoolConstant != null)
        //            {
        //                return SimplifyBoolConstantComparisonExpression(
        //                    operatorType,
        //                    left,
        //                    right,
        //                    leftBoolConstant,
        //                    rightBoolConstant,
        //                    typeMapping);
        //            }

        //            // only works when a is not nullable
        //            // a == a -> true
        //            // a != a -> false
        //            if ((left is LikeExpression
        //                || left is ColumnExpression columnExpression && !columnExpression.IsNullable)
        //                && left.Equals(right))
        //            {
        //                return SqlExpressionFactory.Constant(operatorType == ExpressionType.Equal, typeMapping);
        //            }

        //            break;
        //    }

        //    return SqlExpressionFactory.MakeBinary(operatorType, left, right, typeMapping);
        //}

        protected virtual SqlExpression SimplifyNullComparisonExpression(
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
                }

                if (rightNull)
                {
                    return SimplifyNullNotNullExpression(operatorType, left, typeof(bool), typeMapping);
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
                return operatorType == ExpressionType.Equal
                    ? SqlExpressionFactory.Constant((bool)leftBoolConstant.Value == (bool)rightBoolConstant.Value, typeMapping)
                    : SqlExpressionFactory.Constant((bool)leftBoolConstant.Value != (bool)rightBoolConstant.Value, typeMapping);
            }

            if (rightBoolConstant != null
                && CanOptimize(left))
            {
                // a == true -> a
                // a == false -> !a
                // a != true -> !a
                // a != false -> a
                // only correct when f(x) can't be null
                return operatorType == ExpressionType.Equal
                    ? (bool)rightBoolConstant.Value
                        ? left
                        : SimplifyUnaryExpression(ExpressionType.Not, left, typeof(bool), typeMapping)
                    : (bool)rightBoolConstant.Value
                        ? SimplifyUnaryExpression(ExpressionType.Not, left, typeof(bool), typeMapping)
                        : left;
            }

            if (leftBoolConstant != null
                && CanOptimize(right))
            {
                // true == a -> a
                // false == a -> !a
                // true != a -> !a
                // false != a -> a
                // only correct when a can't be null
                return operatorType == ExpressionType.Equal
                    ? (bool)leftBoolConstant.Value
                        ? right
                        : SimplifyUnaryExpression(ExpressionType.Not, right, typeof(bool), typeMapping)
                    : (bool)leftBoolConstant.Value
                        ? SimplifyUnaryExpression(ExpressionType.Not, right, typeof(bool), typeMapping)
                        : right;
            }

            return SqlExpressionFactory.MakeBinary(operatorType, left, right, typeMapping);

            static bool CanOptimize(SqlExpression operand)
                => operand is LikeExpression
                || (operand is SqlUnaryExpression sqlUnary
                    && (sqlUnary.OperatorType == ExpressionType.Equal
                        || sqlUnary.OperatorType == ExpressionType.NotEqual
                        // TODO: #18689
                        /*|| sqlUnary.OperatorType == ExpressionType.Not*/));
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

        protected override Expression VisitSqlConstant(SqlConstantExpression sqlConstantExpression)
            => sqlConstantExpression;

        protected override Expression VisitSqlFragment(SqlFragmentExpression sqlFragmentExpression)
            => sqlFragmentExpression;

        protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
        {
            var newInstance = (SqlExpression)Visit(sqlFunctionExpression.Instance);
            var newArguments = new SqlExpression[sqlFunctionExpression.Arguments.Count];
            for (var i = 0; i < newArguments.Length; i++)
            {
                newArguments[i] = (SqlExpression)Visit(sqlFunctionExpression.Arguments[i]);
            }

            return sqlFunctionExpression.Update(newInstance, newArguments);
        }

        protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
            => sqlParameterExpression;

        protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
        {
            var newOperand = (SqlExpression)Visit(sqlUnaryExpression.Operand);

            return SimplifyUnaryExpression(
                sqlUnaryExpression.OperatorType,
                newOperand,
                sqlUnaryExpression.Type,
                sqlUnaryExpression.TypeMapping);
        }

        protected virtual SqlExpression SimplifyUnaryExpression(
            ExpressionType operatorType,
            SqlExpression operand,
            Type type,
            RelationalTypeMapping typeMapping)
        {
            switch (operatorType)
            {
                case ExpressionType.Not
                    when type == typeof(bool)
                    || type == typeof(bool?):
                {
                    switch (operand)
                    {
                        // !(true) -> false
                        // !(false) -> true
                        case SqlConstantExpression constantOperand
                            when constantOperand.Value is bool value:
                        {
                            return SqlExpressionFactory.Constant(!value, typeMapping);
                        }

                        case InExpression inOperand:
                            return inOperand.Negate();

                        case SqlUnaryExpression unaryOperand:
                            switch (unaryOperand.OperatorType)
                            {
                                // !(!a) -> a
                                case ExpressionType.Not:
                                    return unaryOperand.Operand;

                                //!(a IS NULL) -> a IS NOT NULL
                                case ExpressionType.Equal:
                                    return SqlExpressionFactory.IsNotNull(unaryOperand.Operand);

                                //!(a IS NOT NULL) -> a IS NULL
                                case ExpressionType.NotEqual:
                                    return SqlExpressionFactory.IsNull(unaryOperand.Operand);
                            }

                            break;

                        //case SqlBinaryExpression binaryOperand:
                        //{
                        //    // De Morgan's
                        //    if (binaryOperand.OperatorType == ExpressionType.AndAlso
                        //        || binaryOperand.OperatorType == ExpressionType.OrElse)
                        //    {
                        //        var newLeft = SimplifyUnaryExpression(ExpressionType.Not, binaryOperand.Left, type, typeMapping);
                        //        var newRight = SimplifyUnaryExpression(ExpressionType.Not, binaryOperand.Right, type, typeMapping);

                        //        return SimplifyLogicalSqlBinaryExpression(
                        //            binaryOperand.OperatorType == ExpressionType.AndAlso
                        //                ? ExpressionType.OrElse
                        //                : ExpressionType.AndAlso,
                        //            newLeft,
                        //            newRight,
                        //            binaryOperand.TypeMapping);
                        //    }

                        //    //// those optimizations are only valid in 2-value logic
                        //    //// they are safe to do here because if we apply null semantics
                        //    //// because null semantics removes possibility of nulls in the tree when the comparison is wrapped around NOT
                        //    //if (!_useRelationalNulls
                        //    //    && TryNegate(binaryOperand.OperatorType, out var negated))
                        //    //{
                        //    //    return SimplifyBinaryExpression(
                        //    //        negated,
                        //    //        binaryOperand.Left,
                        //    //        binaryOperand.Right,
                        //    //        binaryOperand.TypeMapping);
                        //    //}
                        //}
                        //break;
                    }
                    break;
                }

                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                    return SimplifyNullNotNullExpression(
                        operatorType,
                        operand,
                        type,
                        typeMapping);
            }

            return SqlExpressionFactory.MakeUnary(operatorType, operand, type, typeMapping);
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
                case ExpressionType.NotEqual:
                    switch (operand)
                    {
                        case SqlConstantExpression constantOperand:
                            return SqlExpressionFactory.Constant(
                                operatorType == ExpressionType.Equal
                                    ? constantOperand.Value == null
                                    : constantOperand.Value != null,
                                typeMapping);

                        case ColumnExpression columnOperand
                            when !columnOperand.IsNullable:
                            return SqlExpressionFactory.Constant(operatorType == ExpressionType.NotEqual, typeMapping);

                        case SqlUnaryExpression sqlUnaryOperand:
                            if (sqlUnaryOperand.OperatorType == ExpressionType.Convert
                                || sqlUnaryOperand.OperatorType == ExpressionType.Not
                                || sqlUnaryOperand.OperatorType == ExpressionType.Negate)
                            {
                                // op(a) is null -> a is null
                                // op(a) is not null -> a is not null
                                return SimplifyNullNotNullExpression(operatorType, sqlUnaryOperand.Operand, type, typeMapping);
                            }

                            if (sqlUnaryOperand.OperatorType == ExpressionType.Equal
                                || sqlUnaryOperand.OperatorType == ExpressionType.NotEqual)
                            {
                                // (a is null) is null -> false
                                // (a is not null) is null -> false
                                // (a is null) is not null -> true
                                // (a is not null) is not null -> true
                                return SqlExpressionFactory.Constant(operatorType == ExpressionType.NotEqual, typeMapping);
                            }
                            break;

                        case SqlBinaryExpression sqlBinaryOperand:
                            // in general:
                            // binaryOp(a, b) == null -> a == null || b == null
                            // binaryOp(a, b) != null -> a != null && b != null
                            // for coalesce:
                            // (a ?? b) == null -> a == null && b == null
                            // (a ?? b) != null -> a != null || b != null
                            // for AndAlso, OrElse we can't do this optimization
                            // we could do something like this, but it seems too complicated:
                            // (a && b) == null -> a == null && b != 0 || a != 0 && b == null
                            if (sqlBinaryOperand.OperatorType != ExpressionType.AndAlso
                                && sqlBinaryOperand.OperatorType != ExpressionType.OrElse)
                            {
                                var newLeft = SimplifyNullNotNullExpression(operatorType, sqlBinaryOperand.Left, typeof(bool), typeMapping);
                                var newRight = SimplifyNullNotNullExpression(operatorType, sqlBinaryOperand.Right, typeof(bool), typeMapping);

                                return sqlBinaryOperand.OperatorType == ExpressionType.Coalesce
                                    ? SimplifyLogicalSqlBinaryExpression(
                                        operatorType == ExpressionType.Equal
                                            ? ExpressionType.AndAlso
                                            : ExpressionType.OrElse,
                                        newLeft,
                                        newRight,
                                        typeMapping)
                                    : SimplifyLogicalSqlBinaryExpression(
                                        operatorType == ExpressionType.Equal
                                            ? ExpressionType.OrElse
                                            : ExpressionType.AndAlso,
                                        newLeft,
                                        newRight,
                                        typeMapping);
                            }
                            break;
                    }
                    break;
            }

            return SqlExpressionFactory.MakeUnary(operatorType, operand, type, typeMapping);
        }

        protected override Expression VisitTable(TableExpression tableExpression)
            => tableExpression;

        protected override Expression VisitUnion(UnionExpression unionExpression)
        {
            var source1 = (SelectExpression)Visit(unionExpression.Source1);
            var source2 = (SelectExpression)Visit(unionExpression.Source2);

            return unionExpression.Update(source1, source2);
        }
    }
}
