// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public class NullabilityComputingSqlExpressionVisitor : SqlExpressionVisitor
    {
        protected virtual bool Nullable { get; set; }

        protected virtual List<ColumnExpression> NonNullableColumns { get; } = new List<ColumnExpression>();

        protected override Expression VisitCase(CaseExpression caseExpression)
        {
            Nullable = false;
            // if there is no 'else' there is a possibility of null, when none of the conditions are met
            // otherwise the result is nullable if any of the WhenClause results OR ElseResult is nullable
            var isNullable = caseExpression.ElseResult == null;

            var newOperand = (SqlExpression)Visit(caseExpression.Operand);
            var newWhenClauses = new List<CaseWhenClause>();
            foreach (var whenClause in caseExpression.WhenClauses)
            {
                var newTest = (SqlExpression)Visit(whenClause.Test);
                Nullable = false;
                var newResult = (SqlExpression)Visit(whenClause.Result);
                isNullable |= Nullable;
                newWhenClauses.Add(new CaseWhenClause(newTest, newResult));
            }

            var newElseResult = (SqlExpression)Visit(caseExpression.ElseResult);
            Nullable |= isNullable;

            return caseExpression.Update(newOperand, newWhenClauses, newElseResult);
        }

        protected override Expression VisitColumn(ColumnExpression columnExpression)
        {
            Nullable = !NonNullableColumns.Contains(columnExpression) && columnExpression.IsNullable;

            return columnExpression;
        }

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
            Nullable = false;
            var item = (SqlExpression)Visit(inExpression.Item);
            var isNullable = Nullable;
            Nullable = false;
            var subquery = (SelectExpression)Visit(inExpression.Subquery);
            isNullable |= Nullable;
            Nullable = false;
            var values = (SqlExpression)Visit(inExpression.Values);
            Nullable |= isNullable;

            return inExpression.Update(item, values, subquery);
        }

        protected override Expression VisitInnerJoin(InnerJoinExpression innerJoinExpression)
        {
            var newTable = (TableExpressionBase)Visit(innerJoinExpression.Table);
            var newJoinPredicate = (SqlExpression)Visit(innerJoinExpression.JoinPredicate);

            return innerJoinExpression.Update(newTable, newJoinPredicate);
        }

        protected override Expression VisitIntersect(IntersectExpression intersectExpression)
        {
            var source1 = (SelectExpression)Visit(intersectExpression.Source1);
            var source2 = (SelectExpression)Visit(intersectExpression.Source2);

            return intersectExpression.Update(source1, source2);
        }

        protected override Expression VisitLeftJoin(LeftJoinExpression leftJoinExpression)
        {
            var newTable = (TableExpressionBase)Visit(leftJoinExpression.Table);
            var newJoinPredicate = (SqlExpression)Visit(leftJoinExpression.JoinPredicate);

            return leftJoinExpression.Update(newTable, newJoinPredicate);
        }

        protected override Expression VisitLike(LikeExpression likeExpression)
        {
            Nullable = false;
            var newMatch = (SqlExpression)Visit(likeExpression.Match);
            var isNullable = Nullable;
            Nullable = false;
            var newPattern = (SqlExpression)Visit(likeExpression.Pattern);
            isNullable |= Nullable;
            Nullable = false;
            var newEscapeChar = (SqlExpression)Visit(likeExpression.EscapeChar);
            Nullable |= isNullable;

            return likeExpression.Update(newMatch, newPattern, newEscapeChar);
        }

        protected override Expression VisitOrdering(OrderingExpression orderingExpression)
        {
            var expression = (SqlExpression)Visit(orderingExpression.Expression);

            return orderingExpression.Update(expression);
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

            var groupBy = new List<SqlExpression>();
            foreach (var groupingKey in selectExpression.GroupBy)
            {
                var newGroupingKey = (SqlExpression)Visit(groupingKey);
                changed |= newGroupingKey != groupingKey;
                groupBy.Add(newGroupingKey);
            }

            var havingExpression = (SqlExpression)Visit(selectExpression.Having);
            changed |= havingExpression != selectExpression.Having;

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

            var left = (SqlExpression)Visit(sqlBinaryExpression.Left);
            var leftNullable = Nullable;
            var right = (SqlExpression)Visit(sqlBinaryExpression.Right);
            var rightNullable = Nullable;

            Nullable = sqlBinaryExpression.OperatorType == ExpressionType.Coalesce
                ? leftNullable && rightNullable
                : leftNullable || rightNullable;

            return sqlBinaryExpression.Update(left, right);
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
            var newInstance = (SqlExpression)Visit(sqlFunctionExpression.Instance);
            var newArguments = new SqlExpression[sqlFunctionExpression.Arguments.Count];
            for (var i = 0; i < newArguments.Length; i++)
            {
                newArguments[i] = (SqlExpression)Visit(sqlFunctionExpression.Arguments[i]);
            }

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
            var operand = (SqlExpression)Visit(sqlUnaryExpression.Operand);

            // result of IsNull/IsNotNull can never be null
            if (sqlUnaryExpression.OperatorType == ExpressionType.Equal
                || sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                Nullable = false;
            }

            return sqlUnaryExpression.Update(operand);
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
