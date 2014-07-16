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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BootstrapFunctionBinder
{
    private static final AtomicLong NEXT_BINDING_ID = new AtomicLong();

    private final Metadata metadata;

    private final ConcurrentMap<Long, CallSite> bindings = new ConcurrentHashMap<>();

    public BootstrapFunctionBinder(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public FunctionBinding bindFunction(String name, ByteCodeNode getSessionByteCode, List<ByteCodeNode> arguments, FunctionBinder defaultFunctionBinder)
    {
        // perform binding
        FunctionBinding functionBinding = defaultFunctionBinder.bindFunction(NEXT_BINDING_ID.getAndIncrement(), name, getSessionByteCode, arguments);

        // record binding for use by invokedynamic bootstrap call
        bindings.put(functionBinding.getBindingId(), functionBinding.getCallSite());

        return functionBinding;
    }

    public FunctionBinding bindOperator(OperatorType operatorType, ByteCodeNode getSessionByteCode, List<ByteCodeNode> arguments, List<Type> argumentTypes)
    {
        FunctionInfo operatorInfo = metadata.resolveOperator(operatorType, argumentTypes);
        return bindFunction(operatorInfo.getSignature().getName(), getSessionByteCode, arguments, operatorInfo.getFunctionBinder());
    }

    public FunctionBinding bindConstant(Object constant, Class<?> type)
    {
        long bindingId = NEXT_BINDING_ID.getAndIncrement();
        ConstantCallSite callsite = new ConstantCallSite(MethodHandles.constant(type, constant));
        bindings.put(bindingId, callsite);
        return new FunctionBinding(bindingId, "constant_" + bindingId, callsite, ImmutableList.<ByteCodeNode>of(), true);
    }

    public CallSite bootstrap(String name, MethodType type, long bindingId)
    {
        CallSite callSite = bindings.get(bindingId);
        checkArgument(callSite != null, "Binding %s for function %s%s not found", bindingId, name, type.parameterList());

        return callSite;
    }
}
