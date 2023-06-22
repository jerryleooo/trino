package io.trino.hive.functions;

import com.google.inject.Key;
import io.airlift.log.Logger;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingTrinoClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hive.functions.HiveFunctionsTestUtils.createTestingTrinoServer;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class AbstractTestHiveFunctions
{
    private static final Logger log = Logger.get(AbstractTestHiveFunctions.class);

    TestingTrinoServer server;
    protected TestingTrinoClient client;
    protected TypeManager typeManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        // TODO: Use DistributedQueryRunner to perform query
        server = createTestingTrinoServer();
        client = new TestingTrinoClient(server, testSessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Bahia_Banderas"))
                .build());
        typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    private void assertFunction(String expr, Type type, Object value)
    {
        assertQuery("SELECT " + expr, Column.of(type, value));
    }

    private void assertInvalidFunction(String expr, String exceptionPattern)
    {
        try {
            client.execute("SELECT " + expr);
            fail("Function expected to fail but not");
        }
        catch (Exception e) {
            if (!(e.getMessage().matches(exceptionPattern))) {
                fail(format("Expected exception message '%s' to match '%s' but not",
                        e.getMessage(), exceptionPattern));
            }
        }
    }

    private void assertQuery(@Language("SQL") String sql, Column... cols)
    {
        checkArgument(cols != null && cols.length > 0);
        int numColumns = cols.length;
        int numRows = cols[0].values.length;
        checkArgument(Stream.of(cols).allMatch(c -> c != null && c.values.length == numRows));

        MaterializedResult result = client.execute(sql).getResult();
        assertEquals(result.getRowCount(), numRows);

        for (int i = 0; i < numColumns; i++) {
            assertEquals(result.getTypes().get(i), cols[i].type);
        }
        List<MaterializedRow> rows = result.getMaterializedRows();
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numColumns; j++) {
                Object actual = rows.get(i).getField(j);
                Object expected = cols[j].values[i];
                if (cols[j].type == DOUBLE) {
                    assertEquals(((Number) actual).doubleValue(), ((double) expected), 0.000001);
                }
                else {
                    assertEquals(actual, expected);
                }
            }
        }
    }

    protected Optional<File> getInitScript()
    {
        return Optional.empty();
    }

    public static class Column
    {
        private final Type type;

        private final Object[] values;

        public static Column of(Type type, Object... values)
        {
            return new Column(type, values);
        }

        private Column(Type type, Object[] values)
        {
            this.type = type;
            this.values = values;
        }
    }
}
