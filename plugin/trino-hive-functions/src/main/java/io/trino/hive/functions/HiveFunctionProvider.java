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

import io.trino.hive.functions.gen.ScalarMethodHandles;
import io.trino.spi.function.*;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Optional;

public class HiveFunctionProvider implements FunctionProvider {

    private TypeManager typeManager;

    @Inject
    public HiveFunctionProvider(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        Signature signature = FunctionRegistry.getSignature(boundSignature.getName());
        return getHiveScalarFunctionImplementation(signature);
    }

    private ScalarFunctionImplementation getHiveScalarFunctionImplementation(Signature signature)
    {
        // construct a SqlScalarFunction instance and call `specialize`
        MethodHandle methodHandle = ScalarMethodHandles.generateUnbound(signature, typeManager);
        return ScalarFunctionImplementation.builder().methodHandle(methodHandle).build();
    }
}
