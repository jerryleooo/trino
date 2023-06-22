package io.trino.hive.functions;

import io.trino.spi.Plugin;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

public class HiveFunctionsPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of();
    }
}
