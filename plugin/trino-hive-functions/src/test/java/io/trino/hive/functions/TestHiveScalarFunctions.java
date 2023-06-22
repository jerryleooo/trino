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
package io.trino.hive.functions;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.HashSet;

import static com.google.shaded.common.shaded.math.DoubleMath.fuzzyEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static org.testng.Assert.*;

public class TestHiveScalarFunctions extends AbstractTestHiveFunctions {
    private static final String FUNCTION_PREFIX = "hive.default.";
    private static final String TABLE_NAME = "memory.default.function_testing";
    private static final Type INTEGER_ARRAY = new ArrayType(INTEGER);
    private static final Type VARCHAR_ARRAY = new ArrayType(VARCHAR);

    @Test
    public void genericFunction()
    {
        check(select("isnull", "null"), BOOLEAN, true);
        check(select("isnull", "1"), BOOLEAN, false);
        check(selectF("isnull", "c_varchar_null"), BOOLEAN, true);
        check(select("isnotnull", "1"), BOOLEAN, true);
        check(select("isnotnull", "null"), BOOLEAN, false);
        check(selectF("isnotnull", "c_varchar_null"), BOOLEAN, false);
        check(select("nvl", "null", "'2'"), VARCHAR, "2");
        check(selectF("nvl", "c_varchar_null", "'2'"), VARCHAR, "2");

        // Primitive numbers
        check(selectF("abs", "c_bigint"), BIGINT, 1L);
        check(selectF("abs", "c_integer"), INTEGER, 1);
        check(selectF("abs", "c_smallint"), INTEGER, 1);
        check(selectF("abs", "c_tinyint"), INTEGER, 1);
        check(selectF("abs", "c_decimal_52"), createDecimalType(5, 2), BigDecimal.valueOf(12345, 2));
        check(selectF("abs", "c_real"), DOUBLE, 123.45f);
        check(selectF("abs", "c_double"), DOUBLE, 123.45);

        // Primitive string
        check(selectF("upper", "c_varchar"), VARCHAR, "VARCHAR");
        check(selectF("upper", "c_varchar_10"), VARCHAR, "VARCHAR10");
        check(selectF("upper", "c_char_10"), VARCHAR, "CHAR10");
    }

    private void check(@Language("SQL") String query, Type expectedType, Object expectedValue)
    {
        MaterializedResult result = client.execute(query).getResult();
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), expectedType);
        Object actual = result.getMaterializedRows().get(0).getField(0);

        if (expectedType.equals(DOUBLE) || expectedType.equals(RealType.REAL)) {
            if (expectedValue == null) {
                assertNaN(actual);
            }
            else {
                assertTrue(fuzzyEquals(((Number) actual).doubleValue(), ((Number) expectedValue).doubleValue(), 0.000001));
            }
        }
        else {
            assertEquals(actual, expectedValue);
        }
    }

    private Type typeOf(String signature)
    {
        return typeManager.getType(parseTypeSignature(signature, new HashSet<>()));
    }

    private static void assertNaN(Object o)
    {
        if (o instanceof Double) {
            assertEquals((Double) o, Double.NaN);
        }
        else if (o instanceof Float) {
            assertEquals((Float) o, Float.NaN);
        }
        else {
            fail("Unexpected " + o);
        }
    }

    private static String select(String function, String... args)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(FUNCTION_PREFIX).append(function);
        builder.append("(");
        if (args != null) {
            builder.append(String.join(", ", args));
        }
        builder.append(")");
        return builder.toString();
    }

    private static String selectF(String function, String... args)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(FUNCTION_PREFIX).append(function);
        builder.append("(");
        if (args != null) {
            builder.append(String.join(", ", args));
        }
        builder.append(")").append(" FROM ").append(TABLE_NAME);
        return builder.toString();
    }
}
