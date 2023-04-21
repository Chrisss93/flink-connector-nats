package com.github.chrisss93.connector.nats.table;

import org.apache.flink.table.expressions.*;
import org.apache.flink.table.expressions.utils.ResolvedExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.Function;

import static com.github.chrisss93.connector.nats.table.JetStreamConnectorOptions.SUBJECT_FIELD;
import static java.util.Collections.singletonList;
import static java.util.Arrays.asList;

public class SubjectVisitor extends ResolvedExpressionDefaultVisitor<Collection<String>> {
    private static final HashMap<FunctionDefinition, Function<Object, Collection<String>>> VALID_FUNCS;
    static {
        VALID_FUNCS = new HashMap<>();

        VALID_FUNCS.put(BuiltInFunctionDefinitions.EQUALS, (value) -> {
            if (!(value instanceof String)) {
                throw new ExpressionParserException("Function '" +
                    BuiltInFunctionDefinitions.EQUALS + "' is applied to unknown value type: " + value);
            }
            return new ArrayList<>(singletonList((String) value));
        });

        VALID_FUNCS.put(BuiltInFunctionDefinitions.LIKE, (value) -> {
            if (!(value instanceof String)) {
                throw new ExpressionParserException("Function '" +
                    BuiltInFunctionDefinitions.LIKE + "' is applied to unknown value type: " + value);
            }
            String str = (String) value;
            if (str.matches(".*_.*")) {
                // Can't express NATS subject wildcards in terms of the ANSI LIKE underscore character semantics.
                return null;
            }
            return new ArrayList<>(singletonList(str.replaceAll("%", ">")));
        });

        VALID_FUNCS.put(BuiltInFunctionDefinitions.IN, (value) -> {
            if (!(value instanceof String[])) {
                throw new ExpressionParserException("Function '" +
                    BuiltInFunctionDefinitions.IN + "' is applied to unknown value type: " + value);
            }
            return new ArrayList<>(asList((String[]) value));
        });
    }


    @Override
    protected Collection<String> defaultMethod(ResolvedExpression expr) {
        if (!(expr instanceof CallExpression)) {
            return null;
        }
        CallExpression callExpr = (CallExpression) expr;
        // Only handle binary expressions
        if (callExpr.getChildren().size() != 2) {
            return null;
        }

        if (callExpr.getFunctionDefinition() == BuiltInFunctionDefinitions.OR) {
            Collection<String> left = defaultMethod((ResolvedExpression) callExpr.getChildren().get(0));
            Collection<String> right = defaultMethod((ResolvedExpression) callExpr.getChildren().get(1));
            if (left == null || right == null) {
                return null;
            } else {
                left.addAll(right);
                return left;
            }
        }


        String fieldName = null;
        ValueLiteralExpression valueExpr = null;
        switch (valuePosition(callExpr)) {
            case Left:
                fieldName = ((FieldReferenceExpression) callExpr.getChildren().get(1)).getName();
                valueExpr = (ValueLiteralExpression) callExpr.getChildren().get(0);
                break;
            case Right:
                fieldName = ((FieldReferenceExpression) callExpr.getChildren().get(0)).getName();
                valueExpr = (ValueLiteralExpression) callExpr.getChildren().get(1);
                break;
            case Invalid:
                return null;
        }

        // Only handle expressions referencing the SUBJECT_FIELD
        if (!SUBJECT_FIELD.equals(fieldName)) {
            return null;
        }

        Function<Object, Collection<String>> handler = VALID_FUNCS.get(callExpr.getFunctionDefinition());
        // Skip expressions which we do not know how to process.
        if (handler == null) {
            return null;
        }

        Optional<?> valueOpt = valueExpr.getValueAs(valueExpr.getOutputDataType().getConversionClass());
        if (!valueOpt.isPresent()) {
            throw new ExpressionParserException("");
        }

        return handler.apply(valueOpt.get());
    }

    private static ValuePosition valuePosition(CallExpression comp) {
        if (comp.getChildren().size() == 1
            && comp.getChildren().get(0) instanceof FieldReferenceExpression) {
            return ValuePosition.Right;
        } else if (isValue(comp.getChildren().get(0)) && isRef(comp.getChildren().get(1))) {
            return ValuePosition.Left;
        } else if (isRef(comp.getChildren().get(0)) && isValue(comp.getChildren().get(1))) {
            return ValuePosition.Right;
        } else {
            return ValuePosition.Invalid;
        }
    }

    private static boolean isValue(Expression expr) {
        return expr instanceof ValueLiteralExpression;
    }
    private static boolean isRef(Expression expr) {
        return expr instanceof FieldReferenceExpression;
    }

    private enum ValuePosition {
        Left,
        Right,
        Invalid
    }
}
