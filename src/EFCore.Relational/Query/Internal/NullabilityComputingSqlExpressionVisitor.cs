// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public abstract class NullabilityComputingSqlExpressionVisitor : SqlExpressionVisitor
    {
        protected bool Nullable { get; set; }
        protected bool OptimizeNullComparison { get; set; }
        protected List<ColumnExpression> _nonNullableColumns { get; } = new List<ColumnExpression>();

        protected override Expression VisitCase(CaseExpression caseExpression)
        {
            Nullable = false;
            // if there is no 'else' there is a possibility of null, when none of the conditions are met
            // otherwise the result is nullable if any of the WhenClause results OR ElseResult is nullable
            var nullable = caseExpression.ElseResult == null;

            var optimizeNullComparison = OptimizeNullComparison;
            var testIsCondition = caseExpression.Operand == null;
            OptimizeNullComparison = false;
            var newOperand = (SqlExpression)Visit(caseExpression.Operand);
            var newWhenClauses = new List<CaseWhenClause>();
            foreach (var whenClause in caseExpression.WhenClauses)
            {
                OptimizeNullComparison = testIsCondition;
                var newTest = (SqlExpression)Visit(whenClause.Test);
                OptimizeNullComparison = false;
                Nullable = false;
                var newResult = (SqlExpression)Visit(whenClause.Result);
                nullable |= Nullable;
                newWhenClauses.Add(new CaseWhenClause(newTest, newResult));
            }

            OptimizeNullComparison = false;
            var newElseResult = (SqlExpression)Visit(caseExpression.ElseResult);
            Nullable |= nullable;
            OptimizeNullComparison = optimizeNullComparison;

            return caseExpression.Update(newOperand, newWhenClauses, newElseResult);
        }

        protected override Expression VisitColumn(ColumnExpression columnExpression)
        {
            Nullable = !_nonNullableColumns.Contains(columnExpression) && columnExpression.IsNullable;

            return columnExpression;
        }

        protected override Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var table = (TableExpressionBase)Visit(crossApplyExpression.Table);
            OptimizeNullComparison = optimizeNullComparison;

            return crossApplyExpression.Update(table);
        }

        protected override Expression VisitCrossJoin(CrossJoinExpression crossJoinExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var table = (TableExpressionBase)Visit(crossJoinExpression.Table);
            OptimizeNullComparison = optimizeNullComparison;

            return crossJoinExpression.Update(table);
        }

        protected override Expression VisitExcept(ExceptExpression exceptExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var source1 = (SelectExpression)Visit(exceptExpression.Source1);
            var source2 = (SelectExpression)Visit(exceptExpression.Source2);
            OptimizeNullComparison = optimizeNullComparison;

            return exceptExpression.Update(source1, source2);
        }

        protected override Expression VisitExists(ExistsExpression existsExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var newSubquery = (SelectExpression)Visit(existsExpression.Subquery);
            OptimizeNullComparison = optimizeNullComparison;

            return existsExpression.Update(newSubquery);
        }

        protected override Expression VisitFromSql(FromSqlExpression fromSqlExpression)
            => fromSqlExpression;

        protected override Expression VisitIn(InExpression inExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            Nullable = false;
            var item = (SqlExpression)Visit(inExpression.Item);
            var nullable = Nullable;
            Nullable = false;
            var subquery = (SelectExpression)Visit(inExpression.Subquery);
            nullable |= Nullable;
            Nullable = false;
            var values = (SqlExpression)Visit(inExpression.Values);
            Nullable |= nullable;
            OptimizeNullComparison = optimizeNullComparison;

            return inExpression.Update(item, values, subquery);
        }

        protected override Expression VisitInnerJoin(InnerJoinExpression innerJoinExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var newTable = (TableExpressionBase)Visit(innerJoinExpression.Table);
            var newJoinPredicate = VisitJoinPredicate((SqlBinaryExpression)innerJoinExpression.JoinPredicate);
            OptimizeNullComparison = optimizeNullComparison;

            return innerJoinExpression.Update(newTable, newJoinPredicate);
        }

        protected override Expression VisitIntersect(IntersectExpression intersectExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var source1 = (SelectExpression)Visit(intersectExpression.Source1);
            var source2 = (SelectExpression)Visit(intersectExpression.Source2);
            OptimizeNullComparison = optimizeNullComparison;

            return intersectExpression.Update(source1, source2);
        }

        protected override Expression VisitLeftJoin(LeftJoinExpression leftJoinExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var newTable = (TableExpressionBase)Visit(leftJoinExpression.Table);
            var newJoinPredicate = VisitJoinPredicate((SqlBinaryExpression)leftJoinExpression.JoinPredicate);
            OptimizeNullComparison = optimizeNullComparison;

            return leftJoinExpression.Update(newTable, newJoinPredicate);
        }

        private SqlExpression VisitJoinPredicate(SqlBinaryExpression predicate)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = true;

            if (predicate.OperatorType == ExpressionType.Equal)
            {
                var newLeft = (SqlExpression)Visit(predicate.Left);
                var newRight = (SqlExpression)Visit(predicate.Right);
                OptimizeNullComparison = optimizeNullComparison;

                return predicate.Update(newLeft, newRight);
            }

            if (predicate.OperatorType == ExpressionType.AndAlso)
            {
                var newPredicate = (SqlExpression)VisitSqlBinary(predicate);
                OptimizeNullComparison = optimizeNullComparison;

                return newPredicate;
            }

            throw new InvalidOperationException("Unexpected join predicate shape: " + predicate);
        }

        protected override Expression VisitLike(LikeExpression likeExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            Nullable = false;
            var newMatch = (SqlExpression)Visit(likeExpression.Match);
            var nullable = Nullable;
            Nullable = false;
            var newPattern = (SqlExpression)Visit(likeExpression.Pattern);
            nullable |= Nullable;
            Nullable = false;
            var newEscapeChar = (SqlExpression)Visit(likeExpression.EscapeChar);
            Nullable |= nullable;
            OptimizeNullComparison = optimizeNullComparison;

            return likeExpression.Update(newMatch, newPattern, newEscapeChar);
        }

        protected override Expression VisitOrdering(OrderingExpression orderingExpression)
        {
            var expression = (SqlExpression)Visit(orderingExpression.Expression);

            return orderingExpression.Update(expression);
        }

        protected override Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var table = (TableExpressionBase)Visit(outerApplyExpression.Table);
            OptimizeNullComparison = optimizeNullComparison;

            return outerApplyExpression.Update(table);
        }

        protected override Expression VisitProjection(ProjectionExpression projectionExpression)
        {
            var expression = (SqlExpression)Visit(projectionExpression.Expression);

            return projectionExpression.Update(expression);
        }

        protected override Expression VisitRowNumber(RowNumberExpression rowNumberExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var changed = false;
            var partitions = new List<SqlExpression>();
            foreach (var partition in rowNumberExpression.Partitions)
            {
                var newPartition = (SqlExpression)Visit(partition);
                changed |= newPartition != partition;
                partitions.Add(newPartition);
            }

            var orderings = new List<OrderingExpression>();
            foreach (var ordering in rowNumberExpression.Orderings)
            {
                var newOrdering = (OrderingExpression)Visit(ordering);
                changed |= newOrdering != ordering;
                orderings.Add(newOrdering);
            }

            OptimizeNullComparison = optimizeNullComparison;

            return rowNumberExpression.Update(partitions, orderings);
        }

        protected override Expression VisitScalarSubquery(ScalarSubqueryExpression scalarSubqueryExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var subquery = (SelectExpression)Visit(scalarSubqueryExpression.Subquery);
            OptimizeNullComparison = optimizeNullComparison;

            return scalarSubqueryExpression.Update(subquery);
        }

        protected override Expression VisitSelect(SelectExpression selectExpression)
        {
            var changed = false;
            var optimizeNullComparison = OptimizeNullComparison;
            var projections = new List<ProjectionExpression>();
            OptimizeNullComparison = false;
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

            OptimizeNullComparison = true;
            var predicate = (SqlExpression)Visit(selectExpression.Predicate);
            changed |= predicate != selectExpression.Predicate;

            var groupBy = new List<SqlExpression>();
            OptimizeNullComparison = false;
            foreach (var groupingKey in selectExpression.GroupBy)
            {
                var newGroupingKey = (SqlExpression)Visit(groupingKey);
                changed |= newGroupingKey != groupingKey;
                groupBy.Add(newGroupingKey);
            }

            OptimizeNullComparison = true;
            var havingExpression = (SqlExpression)Visit(selectExpression.Having);
            changed |= havingExpression != selectExpression.Having;

            var orderings = new List<OrderingExpression>();
            OptimizeNullComparison = false;
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

            OptimizeNullComparison = optimizeNullComparison;

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
            var optimizeNullComparison = OptimizeNullComparison;

            OptimizeNullComparison = OptimizeNullComparison
                && (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                    || sqlBinaryExpression.OperatorType == ExpressionType.OrElse);

            var nonNullableColumns = new List<ColumnExpression>();
            if (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso)
            {
                nonNullableColumns = FindNonNullableColumns(sqlBinaryExpression.Left);
            }

            var left = (SqlExpression)Visit(sqlBinaryExpression.Left);
            var leftNullable = Nullable;

            Nullable = false;
            if (nonNullableColumns.Count > 0)
            {
                _nonNullableColumns.AddRange(nonNullableColumns);
            }

            var right = (SqlExpression)Visit(sqlBinaryExpression.Right);
            var rightNullable = Nullable;

            foreach (var nonNullableColumn in nonNullableColumns)
            {
                _nonNullableColumns.Remove(nonNullableColumn);
            }

            Nullable = sqlBinaryExpression.OperatorType == ExpressionType.Coalesce
                ? leftNullable && rightNullable
                : leftNullable || rightNullable;

            OptimizeNullComparison = optimizeNullComparison;

            var result = OptimizeSqlBinaryExpression(sqlBinaryExpression.Update(left, right), leftNullable, rightNullable);

            return result;
            //return (result: sqlBinaryExpression.Update(left, right), leftNullable, rightNullable);
        }

        protected virtual SqlExpression OptimizeSqlBinaryExpression(SqlBinaryExpression sqlBinaryExpression, bool leftNullable, bool rightNullable)
            => sqlBinaryExpression;

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

        protected override Expression VisitSqlConstant(SqlConstantExpression sqlConstantExpression)
        {
            Nullable = sqlConstantExpression.Value == null;

            return sqlConstantExpression;
        }

        protected override Expression VisitSqlFragment(SqlFragmentExpression sqlFragmentExpression)
            => sqlFragmentExpression;

        protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;

            var newInstance = (SqlExpression)Visit(sqlFunctionExpression.Instance);
            var newArguments = new SqlExpression[sqlFunctionExpression.Arguments.Count];
            for (var i = 0; i < newArguments.Length; i++)
            {
                newArguments[i] = (SqlExpression)Visit(sqlFunctionExpression.Arguments[i]);
            }

            OptimizeNullComparison = optimizeNullComparison;

            // TODO: #18555
            Nullable = true;

            return sqlFunctionExpression.Update(newInstance, newArguments);
        }

        protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
        {
            // at this point we assume every parameter is nullable, we will filter out the non-nullable ones once we know the actual values
            Nullable = true;

            return sqlParameterExpression;
        }

        protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
        {
            Nullable = false;

            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;

            var operand = (SqlExpression)Visit(sqlUnaryExpression.Operand);
            var operandNullable = Nullable;
            var operandOptimizeNullComparison = OptimizeNullComparison;

            OptimizeNullComparison = optimizeNullComparison;

            var result = OptimizeSqlUnaryExpression(sqlUnaryExpression.Update(operand), operandNullable, operandOptimizeNullComparison);

            // result of IsNull/IsNotNull can never be null
            if (sqlUnaryExpression.OperatorType == ExpressionType.Equal
                || sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                Nullable = false;
            }

            return result;
        }

        protected virtual SqlExpression OptimizeSqlUnaryExpression(
            SqlUnaryExpression sqlUnaryExpression,
            bool operandNullable,
            bool operandOptimizeNullComparison)
            => sqlUnaryExpression;

        protected override Expression VisitTable(TableExpression tableExpression)
            => tableExpression;

        protected override Expression VisitUnion(UnionExpression unionExpression)
        {
            var optimizeNullComparison = OptimizeNullComparison;
            OptimizeNullComparison = false;
            var source1 = (SelectExpression)Visit(unionExpression.Source1);
            var source2 = (SelectExpression)Visit(unionExpression.Source2);
            OptimizeNullComparison = optimizeNullComparison;

            return unionExpression.Update(source1, source2);
        }
    }
}
