// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query
{
    public class NullabilityHandlingExpressionVisitor : SqlExpressionVisitor
    {
        protected virtual bool UseRelationalNulls { get; }
        protected virtual ISqlExpressionFactory SqlExpressionFactory { get; }
        protected virtual IReadOnlyDictionary<string, object> ParameterValues { get; }
        protected virtual List<ColumnExpression> NonNullableColumns { get; } = new List<ColumnExpression>();

        protected virtual bool IsNullable { get; set; }
        protected virtual bool CanOptimize { get; set; }
        protected virtual bool CanCache { get; set; }

        public NullabilityHandlingExpressionVisitor(
            bool useRelationalNulls,
            [NotNull] ISqlExpressionFactory sqlExpressionFactory,
            [NotNull] IReadOnlyDictionary<string, object> parameterValues)
        {
            UseRelationalNulls = useRelationalNulls;
            SqlExpressionFactory = sqlExpressionFactory;
            ParameterValues = parameterValues;

            CanOptimize = true;
            CanCache = true;
        }

        private void RestoreNonNullableColumnsList(int counter)
        {
            if (counter < NonNullableColumns.Count)
            {
                NonNullableColumns.RemoveRange(counter, NonNullableColumns.Count - counter);
            }
        }

        public virtual (SelectExpression selectExpression, bool canCache) HandleNullability(SelectExpression selectExpression)
        {
            var result = (SelectExpression)Visit(selectExpression);

            return (selectExpression: result, canCache: CanCache);
        }

        protected override Expression VisitCase(CaseExpression caseExpression)
        {
            Check.NotNull(caseExpression, nameof(caseExpression));

            IsNullable = false;
            // if there is no 'else' there is a possibility of null, when none of the conditions are met
            // otherwise the result is nullable if any of the WhenClause results OR ElseResult is nullable
            var isNullable = caseExpression.ElseResult == null;

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            var testIsCondition = caseExpression.Operand == null;
            CanOptimize = false;
            var newOperand = (SqlExpression)Visit(caseExpression.Operand);

            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            var newWhenClauses = new List<CaseWhenClause>();
            foreach (var whenClause in caseExpression.WhenClauses)
            {
                CanOptimize = testIsCondition;

                var newTest = (SqlExpression)Visit(whenClause.Test);
                CanOptimize = false;
                IsNullable = false;
                var newResult = (SqlExpression)Visit(whenClause.Result);
                isNullable |= IsNullable;
                newWhenClauses.Add(new CaseWhenClause(newTest, newResult));

                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            CanOptimize = false;
            var newElseResult = (SqlExpression)Visit(caseExpression.ElseResult);
            IsNullable |= isNullable;
            CanOptimize = canOptimize;

            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return caseExpression.Update(newOperand, newWhenClauses, newElseResult);
        }

        protected override Expression VisitColumn(ColumnExpression columnExpression)
        {
            Check.NotNull(columnExpression, nameof(columnExpression));

            IsNullable = !NonNullableColumns.Contains(columnExpression) && columnExpression.IsNullable;

            return columnExpression;
        }

        protected override Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
        {
            Check.NotNull(crossApplyExpression, nameof(crossApplyExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var table = (TableExpressionBase)Visit(crossApplyExpression.Table);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return crossApplyExpression.Update(table);
        }

        protected override Expression VisitCrossJoin(CrossJoinExpression crossJoinExpression)
        {
            Check.NotNull(crossJoinExpression, nameof(crossJoinExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var table = (TableExpressionBase)Visit(crossJoinExpression.Table);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return crossJoinExpression.Update(table);
        }

        protected override Expression VisitExcept(ExceptExpression exceptExpression)
        {
            Check.NotNull(exceptExpression, nameof(exceptExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var source1 = (SelectExpression)Visit(exceptExpression.Source1);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            var source2 = (SelectExpression)Visit(exceptExpression.Source2);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return exceptExpression.Update(source1, source2);
        }

        protected override Expression VisitExists(ExistsExpression existsExpression)
        {
            Check.NotNull(existsExpression, nameof(existsExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var newSubquery = (SelectExpression)Visit(existsExpression.Subquery);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return existsExpression.Update(newSubquery);
        }

        protected override Expression VisitFromSql(FromSqlExpression fromSqlExpression)
        {
            Check.NotNull(fromSqlExpression, nameof(fromSqlExpression));

            return fromSqlExpression;
        }

        protected override Expression VisitIn(InExpression inExpression)
        {
            Check.NotNull(inExpression, nameof(inExpression));

            var canOptimize = CanOptimize;
            CanOptimize = false;
            IsNullable = false;
            var item = (SqlExpression)Visit(inExpression.Item);
            var itemNullable = IsNullable;
            IsNullable = false;

            if (inExpression.Subquery != null)
            {
                var subquery = (SelectExpression)Visit(inExpression.Subquery);
                IsNullable |= itemNullable;
                CanOptimize = canOptimize;

                return inExpression.Update(item, values: null, subquery);
            }

            // for relational null semantics just leave as is
            // same for values we don't know how to properly handle (i.e. other than constant or parameter)
            if (UseRelationalNulls
                || !(inExpression.Values is SqlConstantExpression || inExpression.Values is SqlParameterExpression))
            {
                var values = (SqlExpression)Visit(inExpression.Values);
                IsNullable |= itemNullable;
                CanOptimize = canOptimize;

                return inExpression.Update(item, values, subquery: null);
            }

            // for c# null semantics we need to remove nulls from Values and add IsNull/IsNotNull when necessary
            var (inValues, hasNullValue) = ProcessInExpressionValues(inExpression.Values);

            CanOptimize = canOptimize;

            // either values array is empty or only contains null
            if (((List<object>)inValues.Value).Count == 0)
            {
                IsNullable = false;

                // a IN () -> false
                // non_nullable IN (NULL) -> false
                // a NOT IN () -> true
                // non_nullable NOT IN (NULL) -> true
                // nullable IN (NULL) -> nullable IS NULL
                // nullable NOT IN (NULL) -> nullable IS NOT NULL
                return !hasNullValue || !itemNullable
                    ? (SqlExpression)SqlExpressionFactory.Constant(
                        inExpression.IsNegated,
                        inExpression.TypeMapping)
                    : inExpression.IsNegated
                        ? SqlExpressionFactory.IsNotNull(item)
                        : SqlExpressionFactory.IsNull(item);
            }

            if (!itemNullable
                || (CanOptimize && !inExpression.IsNegated && !hasNullValue))
            {
                IsNullable = itemNullable;

                // non_nullable IN (1, 2) -> non_nullable IN (1, 2)
                // non_nullable IN (1, 2, NULL) -> non_nullable IN (1, 2)
                // non_nullable NOT IN (1, 2) -> non_nullable NOT IN (1, 2)
                // non_nullable NOT IN (1, 2, NULL) -> non_nullable NOT IN (1, 2)
                // nullable IN (1, 2) -> nullable IN (1, 2) (optimized)
                return inExpression.Update(item, inValues, subquery: null);
            }

            // adding null comparison term to remove nulls completely from the resulting expression
            IsNullable = false;

            // nullable IN (1, 2) -> nullable IN (1, 2) AND nullable IS NOT NULL (full)
            // nullable IN (1, 2, NULL) -> nullable IN (1, 2) OR nullable IS NULL (full)
            // nullable NOT IN (1, 2) -> nullable NOT IN (1, 2) OR nullable IS NULL (full)
            // nullable NOT IN (1, 2, NULL) -> nullable NOT IN (1, 2) AND nullable IS NOT NULL (full)
            return inExpression.IsNegated == hasNullValue
                ? SqlExpressionFactory.AndAlso(
                    inExpression.Update(item, inValues, subquery: null),
                    SqlExpressionFactory.IsNotNull(item))
                : SqlExpressionFactory.OrElse(
                    inExpression.Update(item, inValues, subquery: null),
                    SqlExpressionFactory.IsNull(item));

            (SqlConstantExpression processedValues, bool hasNullValue) ProcessInExpressionValues(SqlExpression valuesExpression)
            {
                var inValues = new List<object>();
                var hasNullValue = false;
                RelationalTypeMapping typeMapping = null;

                IEnumerable values = null;
                if (valuesExpression is SqlConstantExpression sqlConstant)
                {
                    typeMapping = sqlConstant.TypeMapping;
                    values = (IEnumerable)sqlConstant.Value;
                }

                if (valuesExpression is SqlParameterExpression sqlParameter)
                {
                    CanCache = false;
                    typeMapping = sqlParameter.TypeMapping;
                    values = (IEnumerable)ParameterValues[sqlParameter.Name];
                }

                foreach (var value in values)
                {
                    if (value == null)
                    {
                        hasNullValue = true;
                        continue;
                    }

                    inValues.Add(value);
                }

                // this is only correct if constant values are the only things allowed here, i.e no mixing of constants and columns
                var processedValues = (SqlConstantExpression)Visit(SqlExpressionFactory.Constant(inValues, typeMapping));

                return (processedValues, hasNullValue);
            }
        }

        protected override Expression VisitInnerJoin(InnerJoinExpression innerJoinExpression)
        {
            Check.NotNull(innerJoinExpression, nameof(innerJoinExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var newTable = (TableExpressionBase)Visit(innerJoinExpression.Table);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            var newJoinPredicate = VisitJoinPredicate((SqlBinaryExpression)innerJoinExpression.JoinPredicate);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return innerJoinExpression.Update(newTable, newJoinPredicate);
        }

        protected override Expression VisitIntersect(IntersectExpression intersectExpression)
        {
            Check.NotNull(intersectExpression, nameof(intersectExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var source1 = (SelectExpression)Visit(intersectExpression.Source1);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            var source2 = (SelectExpression)Visit(intersectExpression.Source2);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return intersectExpression.Update(source1, source2);
        }

        protected override Expression VisitLeftJoin(LeftJoinExpression leftJoinExpression)
        {
            Check.NotNull(leftJoinExpression, nameof(leftJoinExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var newTable = (TableExpressionBase)Visit(leftJoinExpression.Table);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            var newJoinPredicate = VisitJoinPredicate((SqlBinaryExpression)leftJoinExpression.JoinPredicate);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return leftJoinExpression.Update(newTable, newJoinPredicate);
        }

        private SqlExpression VisitJoinPredicate(SqlBinaryExpression predicate)
        {
            var canOptimize = CanOptimize;
            CanOptimize = true;

            if (predicate.OperatorType == ExpressionType.Equal)
            {
                IsNullable = false;
                var left = (SqlExpression)Visit(predicate.Left);
                var leftNullable = IsNullable;
                IsNullable = false;
                var right = (SqlExpression)Visit(predicate.Right);
                var rightNullable = IsNullable;

                var result = OptimizeComparison(
                    predicate.Update(left, right),
                    left,
                    right,
                    leftNullable,
                    rightNullable,
                    CanOptimize);

                CanOptimize = canOptimize;

                return result;
            }

            if (predicate.OperatorType == ExpressionType.AndAlso)
            {
                var newPredicate = (SqlExpression)VisitSqlBinary(predicate);
                CanOptimize = canOptimize;

                return newPredicate;
            }

            throw new InvalidOperationException("Unexpected join predicate shape: " + predicate);
        }

        protected override Expression VisitLike(LikeExpression likeExpression)
        {
            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            IsNullable = false;
            var newMatch = (SqlExpression)Visit(likeExpression.Match);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            var isNullable = IsNullable;
            IsNullable = false;
            var newPattern = (SqlExpression)Visit(likeExpression.Pattern);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            isNullable |= IsNullable;
            IsNullable = false;
            var newEscapeChar = (SqlExpression)Visit(likeExpression.EscapeChar);
            IsNullable |= isNullable;
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return likeExpression.Update(newMatch, newPattern, newEscapeChar);
        }

        protected override Expression VisitOrdering(OrderingExpression orderingExpression)
        {
            Check.NotNull(orderingExpression, nameof(orderingExpression));

            var expression = (SqlExpression)Visit(orderingExpression.Expression);

            return orderingExpression.Update(expression);
        }

        protected override Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
        {
            Check.NotNull(outerApplyExpression, nameof(outerApplyExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var table = (TableExpressionBase)Visit(outerApplyExpression.Table);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return outerApplyExpression.Update(table);
        }

        protected override Expression VisitProjection(ProjectionExpression projectionExpression)
        {
            Check.NotNull(projectionExpression, nameof(projectionExpression));

            var expression = (SqlExpression)Visit(projectionExpression.Expression);

            return projectionExpression.Update(expression);
        }

        protected override Expression VisitRowNumber(RowNumberExpression rowNumberExpression)
        {
            Check.NotNull(rowNumberExpression, nameof(rowNumberExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var changed = false;
            var partitions = new List<SqlExpression>();
            foreach (var partition in rowNumberExpression.Partitions)
            {
                var newPartition = (SqlExpression)Visit(partition);
                changed |= newPartition != partition;
                partitions.Add(newPartition);
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            var orderings = new List<OrderingExpression>();
            foreach (var ordering in rowNumberExpression.Orderings)
            {
                var newOrdering = (OrderingExpression)Visit(ordering);
                changed |= newOrdering != ordering;
                orderings.Add(newOrdering);
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            CanOptimize = canOptimize;

            return rowNumberExpression.Update(partitions, orderings);
        }

        protected override Expression VisitScalarSubquery(ScalarSubqueryExpression scalarSubqueryExpression)
        {
            Check.NotNull(scalarSubqueryExpression, nameof(scalarSubqueryExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var subquery = (SelectExpression)Visit(scalarSubqueryExpression.Subquery);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return scalarSubqueryExpression.Update(subquery);
        }

        protected override Expression VisitSelect(SelectExpression selectExpression)
        {
            Check.NotNull(selectExpression, nameof(selectExpression));

            var changed = false;
            var canOptimize = CanOptimize;
            var projections = new List<ProjectionExpression>();
            CanOptimize = false;

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            foreach (var item in selectExpression.Projection)
            {
                var updatedProjection = (ProjectionExpression)Visit(item);
                projections.Add(updatedProjection);
                changed |= updatedProjection != item;

                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            var tables = new List<TableExpressionBase>();
            foreach (var table in selectExpression.Tables)
            {
                var newTable = (TableExpressionBase)Visit(table);
                changed |= newTable != table;
                tables.Add(newTable);

                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            CanOptimize = true;
            var predicate = (SqlExpression)Visit(selectExpression.Predicate);
            changed |= predicate != selectExpression.Predicate;

            if (predicate is SqlConstantExpression predicateConstantExpression
                && predicateConstantExpression.Value is bool predicateBoolValue
                && predicateBoolValue)
            {
                predicate = null;
                changed = true;
            }

            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            var groupBy = new List<SqlExpression>();
            CanOptimize = false;
            foreach (var groupingKey in selectExpression.GroupBy)
            {
                var newGroupingKey = (SqlExpression)Visit(groupingKey);
                changed |= newGroupingKey != groupingKey;
                groupBy.Add(newGroupingKey);

                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            CanOptimize = true;
            var having = (SqlExpression)Visit(selectExpression.Having);
            changed |= having != selectExpression.Having;

            if (having is SqlConstantExpression havingConstantExpression
                && havingConstantExpression.Value is bool havingBoolValue
                && havingBoolValue)
            {
                having = null;
                changed = true;
            }

            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            var orderings = new List<OrderingExpression>();
            CanOptimize = false;
            foreach (var ordering in selectExpression.Orderings)
            {
                var orderingExpression = (SqlExpression)Visit(ordering.Expression);
                changed |= orderingExpression != ordering.Expression;
                orderings.Add(ordering.Update(orderingExpression));

                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            var offset = (SqlExpression)Visit(selectExpression.Offset);
            changed |= offset != selectExpression.Offset;

            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            var limit = (SqlExpression)Visit(selectExpression.Limit);
            changed |= limit != selectExpression.Limit;

            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            CanOptimize = canOptimize;

            // we assume SelectExpression can always be null
            // (e.g. projecting non-nullable column but with predicate that filters out all rows)
            IsNullable = true;

            return changed
                ? selectExpression.Update(
                    projections, tables, predicate, groupBy, having, orderings, limit, offset, selectExpression.IsDistinct,
                    selectExpression.Alias)
                : selectExpression;
        }

        protected override Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
        {
            Check.NotNull(sqlBinaryExpression, nameof(sqlBinaryExpression));

            IsNullable = false;
            var canOptimize = CanOptimize;

            CanOptimize = CanOptimize && (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                || sqlBinaryExpression.OperatorType == ExpressionType.OrElse);

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var left = (SqlExpression)Visit(sqlBinaryExpression.Left);
            var leftNullable = IsNullable;
            var leftNonNullableColumns = NonNullableColumns.ToList();

            IsNullable = false;

            if (sqlBinaryExpression.OperatorType != ExpressionType.AndAlso)
            {
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            var right = (SqlExpression)Visit(sqlBinaryExpression.Right);
            var rightNullable = IsNullable;
            if (sqlBinaryExpression.OperatorType == ExpressionType.OrElse)
            {
                var intersect = leftNonNullableColumns.Intersect(NonNullableColumns).ToList();
                NonNullableColumns.Clear();
                NonNullableColumns.AddRange(intersect);
            }
            else if (sqlBinaryExpression.OperatorType != ExpressionType.AndAlso)
            {
                // in case of AndAlso we already have what we need as the column information propagates from left to right
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            if (sqlBinaryExpression.OperatorType == ExpressionType.Coalesce)
            {
                IsNullable = leftNullable && rightNullable;
                CanOptimize = canOptimize;

                return sqlBinaryExpression.Update(left, right);
            }

            // nullableStringColumn + NULL -> COALESCE(nullableStringColumn, "") + ""
            if (sqlBinaryExpression.OperatorType == ExpressionType.Add
                && sqlBinaryExpression.Type == typeof(string))
            {
                if (leftNullable)
                {
                    left = AddNullConcatenationProtection(left);
                }

                if (rightNullable)
                {
                    right = AddNullConcatenationProtection(right);
                }

                return sqlBinaryExpression.Update(left, right);
            }

            if (sqlBinaryExpression.OperatorType == ExpressionType.Equal
                || sqlBinaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                var updated = sqlBinaryExpression.Update(left, right);

                var optimized = OptimizeComparison(
                    updated,
                    left,
                    right,
                    leftNullable,
                    rightNullable,
                    canOptimize);

                if (optimized is SqlUnaryExpression optimizedUnary
                    && optimizedUnary.OperatorType == ExpressionType.NotEqual
                    && optimizedUnary.Operand is ColumnExpression optimizedUnaryColumnOperand)
                {
                    NonNullableColumns.Add(optimizedUnaryColumnOperand);
                }

                // we assume that NullSemantics rewrite is only needed (on the current level)
                // if the optimization didn't make any changes.
                // Reason is that optimization can/will change the nullability of the resulting expression
                // and that inforation is not tracked/stored anywhere
                // so we can no longer rely on nullabilities that we computed earlier (leftNullable, rightNullable)
                // when performing null semantics rewrite.
                // It should be fine because current optimizations *radically* change the expression
                // (e.g. binary -> unary, or binary -> constant)
                // but we need to pay attention in the future if we introduce more subtle transformations here
                if (optimized == updated
                    && (leftNullable || rightNullable)
                    && !UseRelationalNulls)
                {
                    var rewriteNullSemanticsResult = RewriteNullSemantics(
                        updated,
                        updated.Left,
                        updated.Right,
                        leftNullable,
                        rightNullable,
                        canOptimize);

                    CanOptimize = canOptimize;

                    return rewriteNullSemanticsResult;
                }

                CanOptimize = canOptimize;

                return optimized;
            }

            IsNullable = leftNullable || rightNullable;
            CanOptimize = canOptimize;

            var result = sqlBinaryExpression.Update(left, right);

            return result is SqlBinaryExpression sqlBinaryResult
                && (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso || sqlBinaryExpression.OperatorType == ExpressionType.OrElse)
                ? SimplifyLogicalSqlBinaryExpression(sqlBinaryResult)
                : result;

            SqlExpression AddNullConcatenationProtection(SqlExpression argument)
                => argument switch
                {
                    SqlConstantExpression _ => SqlExpressionFactory.Constant(string.Empty),
                    SqlParameterExpression _ => SqlExpressionFactory.Constant(string.Empty),
                    ColumnExpression _ => SqlExpressionFactory.Coalesce(argument, SqlExpressionFactory.Constant(string.Empty)),
                    _ => argument
                };
        }

        private SqlExpression OptimizeComparison(
            SqlBinaryExpression sqlBinaryExpression,
            SqlExpression left,
            SqlExpression right,
            bool leftNullable,
            bool rightNullable,
            bool canOptimize)
        {
            var leftNullValue = leftNullable && (left is SqlConstantExpression || left is SqlParameterExpression);
            var rightNullValue = rightNullable && (right is SqlConstantExpression || right is SqlParameterExpression);

            // a == null -> a IS NULL
            // a != null -> a IS NOT NULL
            if (rightNullValue)
            {
                var result = sqlBinaryExpression.OperatorType == ExpressionType.Equal
                    ? ProcessNullNotNull(SqlExpressionFactory.IsNull(left), leftNullable)
                    : ProcessNullNotNull(SqlExpressionFactory.IsNotNull(left), leftNullable);

                IsNullable = false;
                CanOptimize = canOptimize;

                return result;
            }

            // null == a -> a IS NULL
            // null != a -> a IS NOT NULL
            if (leftNullValue)
            {
                var result = sqlBinaryExpression.OperatorType == ExpressionType.Equal
                    ? ProcessNullNotNull(SqlExpressionFactory.IsNull(right), rightNullable)
                    : ProcessNullNotNull(SqlExpressionFactory.IsNotNull(right), rightNullable);

                IsNullable = false;
                CanOptimize = canOptimize;

                return result;
            }

            if (IsTrueOrFalse(right) is bool rightTrueFalseValue
                && !leftNullable)
            {
                IsNullable = leftNullable;
                CanOptimize = canOptimize;

                // only correct in 2-value logic
                // a == true -> a
                // a == false -> !a
                // a != true -> !a
                // a != false -> a
                return sqlBinaryExpression.OperatorType == ExpressionType.Equal ^ rightTrueFalseValue
                    ? SqlExpressionFactory.Not(left)
                    : left;
            }

            if (IsTrueOrFalse(left) is bool leftTrueFalseValue
                && !rightNullable)
            {
                IsNullable = rightNullable;
                CanOptimize = canOptimize;

                // only correct in 2-value logic
                // true == a -> a
                // false == a -> !a
                // true != a -> !a
                // false != a -> a
                return sqlBinaryExpression.OperatorType == ExpressionType.Equal ^ leftTrueFalseValue
                    ? SqlExpressionFactory.Not(right)
                    : right;
            }

            // only correct in 2-value logic
            // a == a -> true
            // a != a -> false
            if (!leftNullable
                && left.Equals(right))
            {
                IsNullable = false;
                CanOptimize = canOptimize;

                return SqlExpressionFactory.Constant(
                    sqlBinaryExpression.OperatorType == ExpressionType.Equal,
                    sqlBinaryExpression.TypeMapping);
            }

            if (!leftNullable
                && !rightNullable
                && (sqlBinaryExpression.OperatorType == ExpressionType.Equal || sqlBinaryExpression.OperatorType == ExpressionType.NotEqual))
            {
                var leftUnary = left as SqlUnaryExpression;
                var rightUnary = right as SqlUnaryExpression;

                var leftNegated = leftUnary?.IsLogicalNot() == true;
                var rightNegated = rightUnary?.IsLogicalNot() == true;

                if (leftNegated)
                {
                    left = leftUnary.Operand;
                }

                if (rightNegated)
                {
                    right = rightUnary.Operand;
                }

                // a == b <=> !a == !b -> a == b
                // !a == b <=> a == !b -> a != b
                // a != b <=> !a != !b -> a != b
                // !a != b <=> a != !b -> a == b
                return sqlBinaryExpression.OperatorType == ExpressionType.Equal ^ leftNegated == rightNegated
                    ? SqlExpressionFactory.NotEqual(left, right)
                    : SqlExpressionFactory.Equal(left, right);
            }

            return sqlBinaryExpression.Update(left, right);

            bool? IsTrueOrFalse(SqlExpression sqlExpression)
            {
                if (sqlExpression is SqlConstantExpression sqlConstantExpression && sqlConstantExpression.Value is bool boolConstant)
                {
                    return boolConstant;
                }

                return null;
            }
        }

        private SqlExpression RewriteNullSemantics(
            SqlBinaryExpression sqlBinaryExpression,
            SqlExpression left,
            SqlExpression right,
            bool leftNullable,
            bool rightNullable,
            bool canOptimize)
        {
            var leftUnary = left as SqlUnaryExpression;
            var rightUnary = right as SqlUnaryExpression;

            var leftNegated = leftUnary?.IsLogicalNot() == true;
            var rightNegated = rightUnary?.IsLogicalNot() == true;

            if (leftNegated)
            {
                left = leftUnary.Operand;
            }

            if (rightNegated)
            {
                right = rightUnary.Operand;
            }

            var leftIsNull = ProcessNullNotNull(SqlExpressionFactory.IsNull(left), leftNullable);
            var rightIsNull = ProcessNullNotNull(SqlExpressionFactory.IsNull(right), rightNullable);

            // optimized expansion which doesn't distinguish between null and false
            if (canOptimize
                && sqlBinaryExpression.OperatorType == ExpressionType.Equal
                && !leftNegated
                && !rightNegated)
            {
                // when we use optimized form, the result can still be nullable
                if (leftNullable && rightNullable)
                {
                    IsNullable = true;
                    CanOptimize = canOptimize;

                    return SqlExpressionFactory.OrElse(
                        SqlExpressionFactory.Equal(left, right),
                        SqlExpressionFactory.AndAlso(leftIsNull, rightIsNull));
                }

                if ((leftNullable && !rightNullable)
                    || (!leftNullable && rightNullable))
                {
                    IsNullable = true;
                    CanOptimize = canOptimize;

                    return SqlExpressionFactory.Equal(left, right);
                }
            }

            // doing a full null semantics rewrite - removing all nulls from truth table
            IsNullable = false;
            CanOptimize = canOptimize;

            if (sqlBinaryExpression.OperatorType == ExpressionType.Equal)
            {
                if (leftNullable && rightNullable)
                {
                    // ?a == ?b <=> !(?a) == !(?b) -> [(a == b) && (a != null && b != null)] || (a == null && b == null))
                    // !(?a) == ?b <=> ?a == !(?b) -> [(a != b) && (a != null && b != null)] || (a == null && b == null)
                    return leftNegated == rightNegated
                        ? ExpandNullableEqualNullable(left, right, leftIsNull, rightIsNull)
                        : ExpandNegatedNullableEqualNullable(left, right, leftIsNull, rightIsNull);
                }

                if (leftNullable && !rightNullable)
                {
                    // ?a == b <=> !(?a) == !b -> (a == b) && (a != null)
                    // !(?a) == b <=> ?a == !b -> (a != b) && (a != null)
                    return leftNegated == rightNegated
                        ? ExpandNullableEqualNonNullable(left, right, leftIsNull)
                        : ExpandNegatedNullableEqualNonNullable(left, right, leftIsNull);
                }

                if (rightNullable && !leftNullable)
                {
                    // a == ?b <=> !a == !(?b) -> (a == b) && (b != null)
                    // !a == ?b <=> a == !(?b) -> (a != b) && (b != null)
                    return leftNegated == rightNegated
                        ? ExpandNullableEqualNonNullable(left, right, rightIsNull)
                        : ExpandNegatedNullableEqualNonNullable(left, right, rightIsNull);
                }
            }

            if (sqlBinaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                if (leftNullable && rightNullable)
                {
                    // ?a != ?b <=> !(?a) != !(?b) -> [(a != b) || (a == null || b == null)] && (a != null || b != null)
                    // !(?a) != ?b <=> ?a != !(?b) -> [(a == b) || (a == null || b == null)] && (a != null || b != null)
                    return leftNegated == rightNegated
                        ? ExpandNullableNotEqualNullable(left, right, leftIsNull, rightIsNull)
                        : ExpandNegatedNullableNotEqualNullable(left, right, leftIsNull, rightIsNull);
                }

                if (leftNullable)
                {
                    // ?a != b <=> !(?a) != !b -> (a != b) || (a == null)
                    // !(?a) != b <=> ?a != !b -> (a == b) || (a == null)
                    return leftNegated == rightNegated
                        ? ExpandNullableNotEqualNonNullable(left, right, leftIsNull)
                        : ExpandNegatedNullableNotEqualNonNullable(left, right, leftIsNull);
                }

                if (rightNullable)
                {
                    // a != ?b <=> !a != !(?b) -> (a != b) || (b == null)
                    // !a != ?b <=> a != !(?b) -> (a == b) || (b == null)
                    return leftNegated == rightNegated
                        ? ExpandNullableNotEqualNonNullable(left, right, rightIsNull)
                        : ExpandNegatedNullableNotEqualNonNullable(left, right, rightIsNull);
                }
            }

            return sqlBinaryExpression.Update(left, right);
        }

        private SqlExpression SimplifyLogicalSqlBinaryExpression(
            SqlBinaryExpression sqlBinaryExpression)
        {
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

            // true && a -> a
            // true || a -> true
            // false && a -> false
            // false || a -> a
            if (sqlBinaryExpression.Left is SqlConstantExpression newLeftConstant)
            {
                return sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                    ? (bool)newLeftConstant.Value
                        ? sqlBinaryExpression.Right
                        : newLeftConstant
                    : (bool)newLeftConstant.Value
                        ? newLeftConstant
                        : sqlBinaryExpression.Right;
            }
            else if (sqlBinaryExpression.Right is SqlConstantExpression newRightConstant)
            {
                // a && true -> a
                // a || true -> true
                // a && false -> false
                // a || false -> a
                return sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                    ? (bool)newRightConstant.Value
                        ? sqlBinaryExpression.Left
                        : newRightConstant
                    : (bool)newRightConstant.Value
                        ? newRightConstant
                        : sqlBinaryExpression.Left;
            }

            return sqlBinaryExpression;
        }

        protected override Expression VisitSqlConstant(SqlConstantExpression sqlConstantExpression)
        {
            Check.NotNull(sqlConstantExpression, nameof(sqlConstantExpression));

            IsNullable = sqlConstantExpression.Value == null;

            return sqlConstantExpression;
        }

        protected override Expression VisitSqlFragment(SqlFragmentExpression sqlFragmentExpression)
        {
            Check.NotNull(sqlFragmentExpression, nameof(sqlFragmentExpression));

            return sqlFragmentExpression;
        }

        protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
        {
            Check.NotNull(sqlFunctionExpression, nameof(sqlFunctionExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;

            var newInstance = (SqlExpression)Visit(sqlFunctionExpression.Instance);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            var newArguments = new SqlExpression[sqlFunctionExpression.Arguments.Count];
            for (var i = 0; i < newArguments.Length; i++)
            {
                newArguments[i] = (SqlExpression)Visit(sqlFunctionExpression.Arguments[i]);
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            CanOptimize = canOptimize;

            // TODO: #18555
            IsNullable = true;

            return sqlFunctionExpression.Update(newInstance, newArguments);
        }

        protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
        {
            Check.NotNull(sqlParameterExpression, nameof(sqlParameterExpression));

            IsNullable = ParameterValues[sqlParameterExpression.Name] == null;

            return IsNullable
                ? SqlExpressionFactory.Constant(null, sqlParameterExpression.TypeMapping)
                : (SqlExpression)sqlParameterExpression;
        }

        protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
        {
            Check.NotNull(sqlUnaryExpression, nameof(sqlUnaryExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            IsNullable = false;
            var canOptimize = CanOptimize;
            CanOptimize = false;

            var operand = (SqlExpression)Visit(sqlUnaryExpression.Operand);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            CanOptimize = canOptimize;
            var updated = sqlUnaryExpression.Update(operand);

            if (sqlUnaryExpression.OperatorType == ExpressionType.Equal
                || sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                // result of IsNull/IsNotNull can never be null
                var isNullable = IsNullable;
                IsNullable = false;

                return ProcessNullNotNull(updated, isNullable);
            }

            return !IsNullable && sqlUnaryExpression.OperatorType == ExpressionType.Not
                ? OptimizeNonNullableNotExpression(updated)
                : updated;
        }

        private SqlExpression OptimizeNonNullableNotExpression(SqlUnaryExpression sqlUnaryExpression)
        {
            var sqlUnaryOperand = sqlUnaryExpression.Operand as SqlUnaryExpression;
            if (sqlUnaryExpression != null)
            {
                fgdfgd
            }


            var sqlBinaryOperand = sqlUnaryExpression.Operand as SqlBinaryExpression;
            if (sqlBinaryOperand == null)
            {
                return sqlUnaryExpression;
            }

            // optimizations below are only correct in 2-value logic
            // De Morgan's
            if (sqlBinaryOperand.OperatorType == ExpressionType.AndAlso
                || sqlBinaryOperand.OperatorType == ExpressionType.OrElse)
            {
                // since entire AndAlso/OrElse expression is non-nullable, both sides of it (left and right) must also be non-nullable
                // so it's safe to perform recursive optimization here
                var left = OptimizeNonNullableNotExpression(SqlExpressionFactory.Not(sqlBinaryOperand.Left));
                var right = OptimizeNonNullableNotExpression(SqlExpressionFactory.Not(sqlBinaryOperand.Right));

                return SimplifyLogicalSqlBinaryExpression(
                    SqlExpressionFactory.MakeBinary(
                        sqlBinaryOperand.OperatorType == ExpressionType.AndAlso
                            ? ExpressionType.OrElse
                            : ExpressionType.AndAlso,
                        left,
                        right,
                        sqlBinaryOperand.TypeMapping));
            }

            // !(a == b) -> a != b
            // !(a != b) -> a == b
            // !(a > b) -> a <= b
            // !(a >= b) -> a < b
            // !(a < b) -> a >= b
            // !(a <= b) -> a > b
            if (TryNegate(sqlBinaryOperand.OperatorType, out var negated))
            {
                return SqlExpressionFactory.MakeBinary(
                    negated,
                    sqlBinaryOperand.Left,
                    sqlBinaryOperand.Right,
                    sqlBinaryOperand.TypeMapping);
            }

            return sqlUnaryExpression;

            static bool TryNegate(ExpressionType expressionType, out ExpressionType result)
            {
                var negated = expressionType switch
                {
                    ExpressionType.Equal => ExpressionType.NotEqual,
                    ExpressionType.NotEqual => ExpressionType.Equal,
                    ExpressionType.GreaterThan => ExpressionType.LessThanOrEqual,
                    ExpressionType.GreaterThanOrEqual => ExpressionType.LessThan,
                    ExpressionType.LessThan => ExpressionType.GreaterThanOrEqual,
                    ExpressionType.LessThanOrEqual => ExpressionType.GreaterThan,
                    _ => (ExpressionType?)null
                };

                result = negated ?? default;

                return negated.HasValue;
            }
        }

        protected virtual SqlExpression ProcessNullNotNull(
            [NotNull] SqlUnaryExpression sqlUnaryExpression,
            bool? operandNullable)
        {
            Check.NotNull(sqlUnaryExpression, nameof(sqlUnaryExpression));

            if (operandNullable == false)
            {
                // when we know that operand is non-nullable:
                // not_null_operand is null-> false
                // not_null_operand is not null -> true
                return SqlExpressionFactory.Constant(
                    sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                    sqlUnaryExpression.TypeMapping);
            }

            switch (sqlUnaryExpression.Operand)
            {
                case SqlConstantExpression sqlConstantOperand:
                    // null_value_constant is null -> true
                    // null_value_constant is not null -> false
                    // not_null_value_constant is null -> false
                    // not_null_value_constant is not null -> true
                    return SqlExpressionFactory.Constant(
                        sqlConstantOperand.Value == null ^ sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                        sqlUnaryExpression.TypeMapping);

                case SqlParameterExpression sqlParameterOperand:
                    // null_value_parameter is null -> true
                    // null_value_parameter is not null -> false
                    // not_null_value_parameter is null -> false
                    // not_null_value_parameter is not null -> true
                    return SqlExpressionFactory.Constant(
                        ParameterValues[sqlParameterOperand.Name] == null ^ sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                        sqlUnaryExpression.TypeMapping);

                case ColumnExpression columnOperand
                    when !columnOperand.IsNullable || NonNullableColumns.Contains(columnOperand):
                {
                    // IsNull(non_nullable_column) -> false
                    // IsNotNull(non_nullable_column) -> true
                    return SqlExpressionFactory.Constant(
                        sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                        sqlUnaryExpression.TypeMapping);
                }

                case SqlUnaryExpression sqlUnaryOperand:
                    switch (sqlUnaryOperand.OperatorType)
                    {
                        case ExpressionType.Convert:
                        case ExpressionType.Not:
                        case ExpressionType.Negate:
                            // op(a) is null -> a is null
                            // op(a) is not null -> a is not null
                            return ProcessNullNotNull(
                                SqlExpressionFactory.MakeUnary(
                                    sqlUnaryExpression.OperatorType,
                                    sqlUnaryOperand.Operand,
                                    sqlUnaryExpression.Type,
                                    sqlUnaryExpression.TypeMapping),
                                operandNullable);

                        case ExpressionType.Equal:
                        case ExpressionType.NotEqual:
                            // (a is null) is null -> false
                            // (a is not null) is null -> false
                            // (a is null) is not null -> true
                            // (a is not null) is not null -> true
                            return SqlExpressionFactory.Constant(
                                sqlUnaryOperand.OperatorType == ExpressionType.NotEqual,
                                sqlUnaryOperand.TypeMapping);
                    }
                    break;

                case SqlBinaryExpression sqlBinaryOperand
                    when sqlBinaryOperand.OperatorType != ExpressionType.AndAlso
                        && sqlBinaryOperand.OperatorType != ExpressionType.OrElse:
                {
                    // in general:
                    // binaryOp(a, b) == null -> a == null || b == null
                    // binaryOp(a, b) != null -> a != null && b != null
                    // for coalesce:
                    // (a ?? b) == null -> a == null && b == null
                    // (a ?? b) != null -> a != null || b != null
                    // for AndAlso, OrElse we can't do this optimization
                    // we could do something like this, but it seems too complicated:
                    // (a && b) == null -> a == null && b != 0 || a != 0 && b == null
                    // NOTE: we don't preserve nullabilities of left/right individually so we are using nullability binary expression as a whole
                    // this may lead to missing some optimizations, where one of the operands (left or right) is not nullable and the other one is
                    var left = ProcessNullNotNull(
                        SqlExpressionFactory.MakeUnary(
                            sqlUnaryExpression.OperatorType,
                            sqlBinaryOperand.Left,
                            typeof(bool),
                            sqlUnaryExpression.TypeMapping),
                        operandNullable: null);

                    var right = ProcessNullNotNull(
                        SqlExpressionFactory.MakeUnary(
                            sqlUnaryExpression.OperatorType,
                            sqlBinaryOperand.Right,
                            typeof(bool),
                            sqlUnaryExpression.TypeMapping),
                        operandNullable: null);

                    return sqlBinaryOperand.OperatorType == ExpressionType.Coalesce
                        ? SqlExpressionFactory.MakeBinary(
                            sqlUnaryExpression.OperatorType == ExpressionType.Equal
                                ? ExpressionType.AndAlso
                                : ExpressionType.OrElse,
                            left,
                            right,
                            sqlUnaryExpression.TypeMapping)
                        : SqlExpressionFactory.MakeBinary(
                            sqlUnaryExpression.OperatorType == ExpressionType.Equal
                                ? ExpressionType.OrElse
                                : ExpressionType.AndAlso,
                            left,
                            right,
                            sqlUnaryExpression.TypeMapping);
                }
            }

            return sqlUnaryExpression;
        }

        protected override Expression VisitTable(TableExpression tableExpression)
        {
            Check.NotNull(tableExpression, nameof(tableExpression));

            return tableExpression;
        }

        protected override Expression VisitUnion(UnionExpression unionExpression)
        {
            Check.NotNull(unionExpression, nameof(unionExpression));

            var currentNonNullableColumnsCount = NonNullableColumns.Count;
            var canOptimize = CanOptimize;
            CanOptimize = false;
            var source1 = (SelectExpression)Visit(unionExpression.Source1);
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            var source2 = (SelectExpression)Visit(unionExpression.Source2);
            CanOptimize = canOptimize;
            RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

            return unionExpression.Update(source1, source2);
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
            => SqlExpressionFactory.OrElse(
                SqlExpressionFactory.AndAlso(
                    SqlExpressionFactory.Equal(left, right),
                    SqlExpressionFactory.AndAlso(
                        SqlExpressionFactory.Not(leftIsNull),
                        SqlExpressionFactory.Not(rightIsNull))),
                SqlExpressionFactory.AndAlso(
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
            => SqlExpressionFactory.OrElse(
                SqlExpressionFactory.AndAlso(
                    SqlExpressionFactory.NotEqual(left, right),
                    SqlExpressionFactory.AndAlso(
                        SqlExpressionFactory.Not(leftIsNull),
                        SqlExpressionFactory.Not(rightIsNull))),
                SqlExpressionFactory.AndAlso(
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
            => SqlExpressionFactory.AndAlso(
                SqlExpressionFactory.Equal(left, right),
                SqlExpressionFactory.Not(leftIsNull));

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
            => SqlExpressionFactory.AndAlso(
                SqlExpressionFactory.NotEqual(left, right),
                SqlExpressionFactory.Not(leftIsNull));

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
            => SqlExpressionFactory.AndAlso(
                SqlExpressionFactory.OrElse(
                    SqlExpressionFactory.NotEqual(left, right),
                    SqlExpressionFactory.OrElse(
                        leftIsNull,
                        rightIsNull)),
                SqlExpressionFactory.OrElse(
                    SqlExpressionFactory.Not(leftIsNull),
                    SqlExpressionFactory.Not(rightIsNull)));

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
            => SqlExpressionFactory.AndAlso(
                SqlExpressionFactory.OrElse(
                    SqlExpressionFactory.Equal(left, right),
                    SqlExpressionFactory.OrElse(
                        leftIsNull,
                        rightIsNull)),
                SqlExpressionFactory.OrElse(
                    SqlExpressionFactory.Not(leftIsNull),
                    SqlExpressionFactory.Not(rightIsNull)));

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
            => SqlExpressionFactory.OrElse(
                SqlExpressionFactory.NotEqual(left, right),
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
            => SqlExpressionFactory.OrElse(
                SqlExpressionFactory.Equal(left, right),
                leftIsNull);
    }
}
