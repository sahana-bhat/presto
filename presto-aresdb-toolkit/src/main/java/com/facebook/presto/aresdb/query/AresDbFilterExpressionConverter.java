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
package com.facebook.presto.aresdb.query;

import com.facebook.presto.aresdb.AresDbException;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Selection;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.aresdb.AresDbUtils.getLiteralAsString;
import static com.facebook.presto.aresdb.query.AresDbExpression.derived;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbFilterExpressionConverter
        implements RowExpressionVisitor<AresDbExpression, Function<VariableReferenceExpression, Selection>>
{
    private static final Set<String> TIME_EQUIVALENT_TYPES = ImmutableSet.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.TINYINT, StandardTypes.SMALLINT);
    private static final String FROM_UNIXTIME = "from_unixtime";
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;

    public AresDbFilterExpressionConverter(
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
    }

    @Override
    public AresDbExpression visitInputReference(InputReferenceExpression reference, Function<VariableReferenceExpression, Selection> context)
    {
        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "AresDb does not support struct dereferencing " + reference);
    }

    @Override
    public AresDbExpression visitVariableReference(VariableReferenceExpression reference, Function<VariableReferenceExpression, Selection> context)
    {
        Selection input = requireNonNull(context.apply(reference), format("Input column %s does not exist in the input: %s", reference, context));
        return new AresDbExpression(input.getDefinition(), Optional.empty(), input.getOrigin());
    }

    @Override
    public AresDbExpression visitConstant(ConstantExpression literal, Function<VariableReferenceExpression, Selection> context)
    {
        return new AresDbExpression(getLiteralAsString(literal), Optional.empty(), AresDbQueryGeneratorContext.Origin.LITERAL);
    }

    @Override
    public AresDbExpression visitLambda(LambdaDefinitionExpression lambda, Function<VariableReferenceExpression, Selection> context)
    {
        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "AresDb does not support lambda " + lambda);
    }

    @Override
    public AresDbExpression visitCall(CallExpression call, Function<VariableReferenceExpression, Selection> context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            return handleNot(call, context);
        }
        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            return handleCast(call, context);
        }
        if (standardFunctionResolution.isBetweenFunction(functionHandle)) {
            return handleBetween(call, context);
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isArithmeticOperator()) {
                return handleArithmeticExpression(call, operatorType, context);
            }
            if (operatorType.isComparisonOperator()) {
                return handleLogicalBinary(operatorType.getOperator(), call, context);
            }
        }
        return handleFunction(call, context);
    }

    @Override
    public AresDbExpression visitSpecialForm(SpecialFormExpression specialForm, Function<VariableReferenceExpression, Selection> context)
    {
        switch (specialForm.getForm()) {
            case IF:
            case NULL_IF:
            case SWITCH:
            case WHEN:
            case IS_NULL:
            case COALESCE:
            case DEREFERENCE:
            case ROW_CONSTRUCTOR:
            case BIND:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "Aresdb does not support the special form " + specialForm);
            case IN:
                return handleIn(specialForm, true, context);
            case AND:
            case OR:
                return derived(format(
                        "(%s %s %s)",
                        specialForm.getArguments().get(0).accept(this, context).getDefinition(),
                        specialForm.getForm().toString(),
                        specialForm.getArguments().get(1).accept(this, context).getDefinition()));
            default:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "Unexpected special form: " + specialForm);
        }
    }

    private AresDbExpression handleIn(
            SpecialFormExpression specialForm,
            boolean whiteListed,
            Function<VariableReferenceExpression, Selection> context)
    {
        String expression = format("(%s IN (%s))",
                specialForm.getArguments().get(0).accept(this, context).getDefinition(),
                specialForm.getArguments().subList(1, specialForm.getArguments().size()).stream()
                        .map(argument -> argument.accept(this, context).getDefinition())
                        .collect(Collectors.joining(", ")));
        return whiteListed ? derived(expression) : derived("! " + expression);
    }

    private AresDbExpression handleLogicalBinary(
            String operator,
            CallExpression call,
            Function<VariableReferenceExpression, Selection> context)
    {
        List<RowExpression> arguments = call.getArguments();
        if (arguments.size() == 2) {
            return derived(format(
                    "(%s %s %s)",
                    arguments.get(0).accept(this, context).getDefinition(),
                    operator,
                    arguments.get(1).accept(this, context).getDefinition()));
        }
        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, format("Unknown logical binary: '%s'", call));
    }

    private AresDbExpression handleBetween(
            CallExpression between,
            Function<VariableReferenceExpression, Selection> context)
    {
        if (between.getArguments().size() == 3) {
            RowExpression value = between.getArguments().get(0);
            RowExpression min = between.getArguments().get(1);
            RowExpression max = between.getArguments().get(2);
            String val = value.accept(this, context).getDefinition();
            return derived(format(
                    "((%s >= %s) AND (%s <= %s))",
                    val,
                    min.accept(this, context).getDefinition(),
                    val,
                    max.accept(this, context).getDefinition()));
        }
        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, format("Between operator not supported: %s", between));
    }

    private AresDbExpression handleNot(CallExpression not, Function<VariableReferenceExpression, Selection> context)
    {
        // TODO check if AresdB supports only NOT on IN ?
        if (not.getArguments().size() == 1) {
            RowExpression input = not.getArguments().get(0);
            if (input instanceof SpecialFormExpression) {
                SpecialFormExpression specialFormExpression = (SpecialFormExpression) input;
                // NOT operator is only supported on top of the IN expression
                if (specialFormExpression.getForm() == SpecialFormExpression.Form.IN) {
                    return handleIn(specialFormExpression, false, context);
                }
            }
        }

        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, format("NOT operator is supported only on top of IN operator. Received: %s", not));
    }

    private AresDbExpression handleCast(CallExpression cast, Function<VariableReferenceExpression, Selection> context)
    {
        if (cast.getArguments().size() == 1) {
            RowExpression input = cast.getArguments().get(0);
            Type expectedType = cast.getType();
            if (typeManager.canCoerce(input.getType(), expectedType)) {
                return input.accept(this, context);
            }
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "Non implicit casts not supported: " + cast);
        }

        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, format("This type of CAST operator not supported. Received: %s", cast));
    }

    private AresDbExpression handleArithmeticExpression(
            CallExpression expression,
            OperatorType operatorType,
            Function<VariableReferenceExpression, Selection> context)
    {
        List<RowExpression> arguments = expression.getArguments();
        if (arguments.size() == 1) {
            String prefix = operatorType == OperatorType.NEGATION ? "-" : "";
            return derived(prefix + arguments.get(0).accept(this, context).getDefinition());
        }
        if (arguments.size() == 2) {
            AresDbExpression left = arguments.get(0).accept(this, context);
            AresDbExpression right = arguments.get(1).accept(this, context);
            String prestoOperator = operatorType.getOperator();
            return derived(format("%s %s %s", left.getDefinition(), prestoOperator, right.getDefinition()));
        }
        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, format("Don't know how to interpret %s as an arithmetic expression", expression));
    }

    private AresDbExpression handleFunction(
            CallExpression function,
            Function<VariableReferenceExpression, Selection> context)
    {
        switch (function.getDisplayName().toLowerCase(ENGLISH)) {
            case "date_trunc":
                return handleDateTrunc(function, context);
            default:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, format("function %s not supported yet", function.getDisplayName()));
        }
    }

    private AresDbExpression handleDateTrunc(CallExpression function, Function<VariableReferenceExpression, Selection> context)
    {
        RowExpression timeInputParameter = function.getArguments().get(1);
        String inputColumn;
        String inputTimeZone;

        CallExpression timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter);
        switch (timeConversion.getDisplayName().toLowerCase(ENGLISH)) {
            case FROM_UNIXTIME:
                inputColumn = timeConversion.getArguments().get(0).accept(this, context).getDefinition();
                inputTimeZone = timeConversion.getArguments().size() > 1 ? getStringFromConstant(timeConversion.getArguments().get(1)) : DateTimeZone.UTC.getID();
                break;
            default:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "not supported: " + timeConversion.getDisplayName());
        }

        RowExpression intervalParameter = function.getArguments().get(0);
        if (!(intervalParameter instanceof ConstantExpression)) {
            throw new AresDbException(
                    ARESDB_UNSUPPORTED_EXPRESSION,
                    null,
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        String intervalUnit = getStringFromConstant(intervalParameter);
        switch (intervalUnit.toLowerCase(ENGLISH)) {
            case "second":
            case "minute":
            case "hour":
            case "day":
            case "week":
            case "month":
            case "quarter":
            case "year":
                break;
            default:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "interval in date_trunc is not supported: " + intervalUnit);
        }

        return derived(inputColumn, new TimeSpec(intervalUnit, TimeZoneKey.getTimeZoneKey(inputTimeZone)));
    }

    private CallExpression getExpressionAsFunction(
            RowExpression originalExpression,
            RowExpression expression)
    {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            if (standardFunctionResolution.isCastFunction(call.getFunctionHandle())) {
                if (isImplicitCast(call.getArguments().get(0).getType(), call.getType())) {
                    return getExpressionAsFunction(originalExpression, call.getArguments().get(0));
                }
            }
            else {
                return call;
            }
        }
        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "Could not dig function out of expression: " + originalExpression + ", inside of " + expression);
    }

    protected boolean isImplicitCast(Type inputType, Type resultType)
    {
        if (typeManager.canCoerce(inputType, resultType)) {
            return true;
        }
        return resultType.getTypeSignature().getBase().equals(StandardTypes.TIMESTAMP) && TIME_EQUIVALENT_TYPES.contains(inputType.getTypeSignature().getBase());
    }

    private static String getStringFromConstant(RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            Object value = ((ConstantExpression) expression).getValue();
            if (value instanceof String) {
                return (String) value;
            }
            if (value instanceof Slice) {
                return ((Slice) value).toStringUtf8();
            }
        }
        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, null, "Expected string literal but found " + expression);
    }
}
