/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class JoinNode
        extends PlanNode
{
    private final Type type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> criteria;
    private final Optional<Symbol> leftHashSymbol;
    private final Optional<Symbol> rightHashSymbol;
    private Expression comparisons;

    @JsonCreator
    public JoinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("leftHashSymbol") Optional<Symbol> leftHashSymbol,
            @JsonProperty("rightHashSymbol") Optional<Symbol> rightHashSymbol)
    {
        super(id);
        checkNotNull(type, "type is null");
        checkNotNull(left, "left is null");
        checkNotNull(right, "right is null");
        checkNotNull(criteria, "criteria is null");
        checkNotNull(leftHashSymbol, "leftHashSymbol is null");
        checkNotNull(rightHashSymbol, "rightHashSymbol is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.leftHashSymbol = leftHashSymbol;
        this.rightHashSymbol = rightHashSymbol;
    }
    
    
	@JsonCreator
    public JoinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
              @JsonProperty("leftHashSymbol") Optional<Symbol> leftHashSymbol,
            @JsonProperty("rightHashSymbol") Optional<Symbol> rightHashSymbol,
            @JsonProperty("comparisons") @Nullable Expression comparisons)
    {
        this(id, type, left, right, criteria,leftHashSymbol,rightHashSymbol);
        this.comparisons=comparisons;
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        CROSS("CrossJoin"),
        FULL("FullJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public static Type typeConvert(Join.Type joinType)
        {
            // Omit SEMI join types because they must be inferred by the planner and not part of the SQL parse tree
            switch (joinType) {
                case INNER:
                    return Type.INNER;
                case LEFT:
                    return Type.LEFT;
                case RIGHT:
                    return Type.RIGHT;
                case FULL:
                    return Type.FULL;
                case CROSS:
                case IMPLICIT:
                    return Type.CROSS;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + joinType);
            }
        }
    }

    @JsonProperty("type")
    public Type getType()
    {
        return type;
    }

    @JsonProperty("left")
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty("right")
    public PlanNode getRight()
    {
        return right;
    }

    @JsonProperty("criteria")
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty("leftHashSymbol")
    public Optional<Symbol> getLeftHashSymbol()
    {
        return leftHashSymbol;
    }

    @JsonProperty("rightHashSymbol")
    public Optional<Symbol> getRightHashSymbol()
    {
        return rightHashSymbol;
    }

    public ComparisonClause getComparasionClauses()
    {
        if (comparisons != null) {
            if (!(comparisons instanceof BooleanLiteral)) {
                return new ComparisonClause(comparisons);
            }
            else {
                if (BooleanLiteral.TRUE_LITERAL.equals((BooleanLiteral) comparisons)) {
                    return null;
                }
                else {
                    throw new RuntimeException("Should not happen.Wrong Logic");
                }
            }
        }
        else {
            return null;
        }
    }
    
    @JsonProperty("comparisons")
    public Expression getComparisons() {
		return comparisons;
	}
    
    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(left.getOutputSymbols())
                .addAll(right.getOutputSymbols())
                .build();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    public static class EquiJoinClause
    {
        private final Symbol left;
        private final Symbol right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") Symbol left, @JsonProperty("right") Symbol right)
        {
            this.left = checkNotNull(left, "left is null");
            this.right = checkNotNull(right, "right is null");
        }

        @JsonProperty("left")
        public Symbol getLeft()
        {
            return left;
        }

        @JsonProperty("right")
        public Symbol getRight()
        {
            return right;
        }
    }
    
    public static class ComparisonClause
    {
        private List<Symbol> left=new ArrayList<Symbol>();
        private List<Symbol> right=new ArrayList<Symbol>();
        private List<ComparisonExpression.Type> types=new ArrayList<ComparisonExpression.Type>();
        public ComparisonClause(Expression e)
        {
            for (Expression conjunct : ExpressionUtils.extractConjuncts((Expression) e)) {
        		ComparisonExpression comparisonExpression=(ComparisonExpression)conjunct;
        		QualifiedNameReference q=((QualifiedNameReference)comparisonExpression.getLeft());
        		left.add(new Symbol(((QualifiedNameReference)comparisonExpression.getLeft()).toString().replace("\"", "")));
        		right.add(new Symbol(((QualifiedNameReference)comparisonExpression.getRight()).toString().replace("\"", "")));
        		types.add(comparisonExpression.getType());
        	}		
        }
        

        public List<Symbol> getLeft()
        {
            return left;
        }

        public List<Symbol> getRight()
        {
            return right;
        }
        
        public List<ComparisonExpression.Type> getTypes()
        {
            return types;
        }

        public static Function<ComparisonClause, List<Symbol>> leftGetter()
        {
            return new Function<ComparisonClause, List<Symbol>>()
            {
                @Override
                public List<Symbol> apply(ComparisonClause input)
                {
                    return input.getLeft();
                }
            };
        }

        public static Function<ComparisonClause, List<Symbol>> rightGetter()
        {
            return new Function<ComparisonClause, List<Symbol>>()
            {
                @Override
                public List<Symbol> apply(ComparisonClause input)
                {
                    return input.getRight();
                }
            };
        }
    }
}
