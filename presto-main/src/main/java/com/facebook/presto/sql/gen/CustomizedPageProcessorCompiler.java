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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.OpCode.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.facebook.presto.sql.gen.ByteCodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

/**
 * 
 * @author dilip kasana
 * @Date 13-Feb-2015
 */
public class CustomizedPageProcessorCompiler implements
		BodyCompiler<PageProcessor> {
	private final Metadata metadata;

	public CustomizedPageProcessorCompiler(Metadata metadata) {
		this.metadata = metadata;
	}

	@Override
	public void generateMethods(ClassDefinition classDefinition,
			CallSiteBinder callSiteBinder, RowExpression filter,
			List<RowExpression> projections) {
		generateProcessMethod(classDefinition, filter, projections);
		generateFilterMethod(classDefinition, callSiteBinder, filter);

		for (int i = 0; i < projections.size(); i++) {
			generateProjectMethod(classDefinition, callSiteBinder, "project_"
					+ i, projections.get(i));
		}
	}

	private void generateProcessMethod(ClassDefinition classDefinition,
			RowExpression filter, List<RowExpression> projections) {
		Parameter session = arg("session", ConnectorSession.class);
		Parameter page = arg("page", Page.class);
		Parameter start = arg("start", int.class);
		Parameter end = arg("end", int.class);
		Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
		MethodDefinition method = classDefinition.declareMethod(a(PUBLIC),
				"process", type(int.class), session, page, start, end,
				pageBuilder);

		Scope scope = method.getScope();
		Variable thisVariable = method.getThis();
		Variable position = scope.declareVariable(int.class, "position");

		method.getBody().comment("int position = start;").getVariable(start)
				.putVariable(position);

		List<Integer> allInputChannels = getInputChannels(Iterables.concat(
				projections, ImmutableList.of(filter)));
		for (int channel : allInputChannels) {
			Variable blockVariable = scope.declareVariable(
					com.facebook.presto.spi.block.Block.class, "block_"
							+ channel);
			method.getBody()
					.comment("Block %s = page.getBlock(%s);",
							blockVariable.getName(), channel)
					.getVariable(page)
					.push(channel)
					.invokeVirtual(Page.class, "getBlock",
							com.facebook.presto.spi.block.Block.class,
							int.class).putVariable(blockVariable);
		}

		//
		// for loop loop body
		//
		LabelNode done = new LabelNode("done");

		Block loopBody = new Block();

		ForLoop loop = new ForLoop()
				.initialize(NOP)
				.condition(
						new Block()
								.comment("position < end")
								.getVariable(position)
								.getVariable(end)
								.invokeStatic(CompilerOperations.class,
										"lessThan", boolean.class, int.class,
										int.class))
				.update(new Block().comment("position++").incrementVariable(
						position, (byte) 1)).body(loopBody);

		loopBody.comment("if (pageBuilder.isFull()) break;")
				.getVariable(pageBuilder)
				.invokeVirtual(PageBuilder.class, "isFull", boolean.class)
				.ifTrueGoto(done);

		// if (filter(cursor))
		IfStatement filterBlock = new IfStatement();
		filterBlock
				.condition()
				.append(thisVariable)
				.getVariable(session)
				.append(pushBlockVariables(scope, getInputChannels(filter)))
				.getVariable(position)
				.invokeVirtual(
						classDefinition.getType(),
						"filter",
						type(boolean.class),
						ImmutableList
								.<ParameterizedType> builder()
								.add(type(ConnectorSession.class))
								.addAll(nCopies(
										getInputChannels(filter).size(),
										type(com.facebook.presto.spi.block.Block.class)))
								.add(type(int.class)).build());

		filterBlock
				.ifTrue()
				.append(pageBuilder)
				.invokeVirtual(PageBuilder.class, "declarePosition", void.class);

		for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
			List<Integer> inputChannels = getInputChannels(projections
					.get(projectionIndex));

			filterBlock.ifTrue().append(thisVariable).append(session)
					.append(pushBlockVariables(scope, inputChannels))
					.getVariable(position);

			filterBlock
					.ifTrue()
					.comment("pageBuilder.getBlockBuilder(%d)", projectionIndex)
					.append(pageBuilder)
					.push(projectionIndex)
					.invokeVirtual(PageBuilder.class, "getBlockBuilder",
							BlockBuilder.class, int.class);

			filterBlock
					.ifTrue()
					.comment(
							"project_%d(session, block_%s, position, blockBuilder)",
							projectionIndex, inputChannels)
					.invokeVirtual(
							classDefinition.getType(),
							"project_" + projectionIndex,
							type(void.class),
							ImmutableList
									.<ParameterizedType> builder()
									.add(type(ConnectorSession.class))
									.addAll(nCopies(
											inputChannels.size(),
											type(com.facebook.presto.spi.block.Block.class)))
									.add(type(int.class))
									.add(type(BlockBuilder.class)).build());
		}

		loopBody.append(filterBlock);

		method.getBody().append(loop).visitLabel(done)
				.comment("return position;").getVariable(position).retInt();
	}

	private void generateFilterMethod(ClassDefinition classDefinition,
			CallSiteBinder callSiteBinder, RowExpression filter) {
		Parameter session = arg("session", ConnectorSession.class);
		List<Parameter> blocks = toBlockParameters(getInputChannels(filter));
		Parameter position = arg("position", int.class);
		MethodDefinition method = classDefinition.declareMethod(a(PUBLIC),
				"filter", type(boolean.class),
				ImmutableList.<Parameter> builder().add(session).addAll(blocks)
						.add(position).build());

		method.comment("Filter: %s", filter.toString());

		Scope scope = method.getScope();
		Variable wasNullVariable = scope.declareVariable(type(boolean.class),
				"wasNull");

		ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(
				callSiteBinder, fieldReferenceCompiler(callSiteBinder,
						position, wasNullVariable),
				metadata.getFunctionRegistry());
		ByteCodeNode body = filter.accept(visitor, scope);

		LabelNode end = new LabelNode("end");
		method.getBody().comment("boolean wasNull = false;")
				.putVariable(wasNullVariable, false).append(body)
				.getVariable(wasNullVariable).ifFalseGoto(end)
				.pop(boolean.class).push(false).visitLabel(end).retBoolean();
	}

	private void generateProjectMethod(ClassDefinition classDefinition,
			CallSiteBinder callSiteBinder, String methodName,
			RowExpression projection) {
		Parameter session = arg("session", ConnectorSession.class);
		List<Parameter> inputs = toBlockParameters(getInputChannels(projection));
		Parameter position = arg("position", int.class);
		Parameter output = arg("output", BlockBuilder.class);
		MethodDefinition method = classDefinition.declareMethod(a(PUBLIC),
				methodName, type(void.class),
				ImmutableList.<Parameter> builder().add(session).addAll(inputs)
						.add(position).add(output).build());

		method.comment("Projection: %s", projection.toString());

		Scope scope = method.getScope();
		Variable wasNullVariable = scope.declareVariable(type(boolean.class),
				"wasNull");

		Block body = method.getBody().comment("boolean wasNull = false;")
				.putVariable(wasNullVariable, false);

		ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(
				callSiteBinder, fieldReferenceCompiler(callSiteBinder,
						position, wasNullVariable),
				metadata.getFunctionRegistry());

		body.getVariable(output)
				.comment("evaluate projection: " + projection.toString())
				.append(projection.accept(visitor, scope))
				.append(generateWrite(callSiteBinder, scope, wasNullVariable,
						projection.getType())).ret();
	}

	private static List<Integer> getInputChannels(
			Iterable<RowExpression> expressions) {
		TreeSet<Integer> channels = new TreeSet<>();
		for (RowExpression expression : Expressions.subExpressions(expressions)) {
			if (expression instanceof InputReferenceExpression) {
				channels.add(((InputReferenceExpression) expression).getField());
			}
		}
		return ImmutableList.copyOf(channels);
	}

	private static List<Integer> getInputChannels(RowExpression expression) {
		return getInputChannels(ImmutableList.of(expression));
	}

	private static List<Parameter> toBlockParameters(List<Integer> inputChannels) {
		ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
		for (int channel : inputChannels) {
			parameters.add(arg("block_" + channel,
					com.facebook.presto.spi.block.Block.class));
		}
		return parameters.build();
	}

	private static ByteCodeNode pushBlockVariables(Scope scope,
			List<Integer> inputs) {
		Block block = new Block();
		for (int channel : inputs) {
			block.append(scope.getVariable("block_" + channel));
		}
		return block;
	}

	private RowExpressionVisitor<Scope, ByteCodeNode> fieldReferenceCompiler(
			final CallSiteBinder callSiteBinder,
			final Variable positionVariable, final Variable wasNullVariable) {
		return new RowExpressionVisitor<Scope, ByteCodeNode>() {
			@Override
			public ByteCodeNode visitInputReference(
					InputReferenceExpression node, Scope scope) {
				int field = node.getField();
				Type type = node.getType();
				Variable block = scope.getVariable("block_" + field);

				Class<?> javaType = type.getJavaType();
				IfStatement ifStatement = new IfStatement();
				ifStatement
						.condition()
						.setDescription(format("block_%d.get%s()", field, type))
						.append(block)
						.getVariable(positionVariable)
						.invokeInterface(
								com.facebook.presto.spi.block.Block.class,
								"isNull", boolean.class, int.class);

				ifStatement.ifTrue().putVariable(wasNullVariable, true)
						.pushJavaDefault(javaType);

				String methodName = "get"
						+ Primitives.wrap(javaType).getSimpleName();

				ifStatement
						.ifFalse()
						.append(loadConstant(callSiteBinder.bind(type,
								Type.class)))
						.append(block)
						.getVariable(positionVariable)
						.invokeInterface(Type.class, methodName, javaType,
								com.facebook.presto.spi.block.Block.class,
								int.class);

				return ifStatement;
			}

			@Override
			public ByteCodeNode visitCall(CallExpression call, Scope scope) {
				throw new UnsupportedOperationException("not yet implemented");
			}

			@Override
			public ByteCodeNode visitConstant(ConstantExpression literal,
					Scope scope) {
				throw new UnsupportedOperationException("not yet implemented");
			}
		};
	}

	public void generateMethods(ClassDefinition classDefinition,
			CallSiteBinder callSiteBinder, RowExpression filter, int offset) {
		generateFilterMethod(classDefinition, callSiteBinder, filter, offset);//@Todo:Dilip Uncomment this
		// generateFilterMethod(classDefinition, callSiteBinder, filter);
	}

	private void generateFilterMethod(ClassDefinition classDefinition,
			CallSiteBinder callSiteBinder, RowExpression filter, int offset) {

		Parameter position1 = arg("position1", int.class);
		Parameter position2 =arg("position2", int.class);
		Parameter buildIndex =arg("buildIndex", int.class);
		MethodDefinition method = classDefinition
				.declareMethod(
						a(PUBLIC),
						"applyFilter",
						type(boolean.class),
						ImmutableList.<Parameter> builder()
								.add(arg("session", ConnectorSession.class))
								.add(arg(
										"probeBlocks",
										type(List.class,
												com.facebook.presto.spi.block.Block.class)))
								// buildBlocks
								.add(arg(
										"buildBlocks",
										type(List.class,
												type(List.class,
														com.facebook.presto.spi.block.Block.class))))
								// .addAll(toBlockParameters(getInputChannels(filter)))
								.add(position1)
								.add(position2)
								.add(buildIndex).build());
		// int channelSize=getInputChannels(filter).size();
		// List<FieldDefinition> blockFields = new ArrayList<>();
		// for (int i = 0; i < channelSize; i++) {
		// FieldDefinition channelField;
		// // if(i==0)
		// // channelField = classDefinition.declareField(a(PRIVATE), "block" ,
		// com.facebook.presto.spi.block.Block.class);
		// // else{
		// channelField = classDefinition.declareField(a(PRIVATE), "block_" + i,
		// com.facebook.presto.spi.block.Block.class);
		//
		// //}
		// blockFields.add(channelField);
		// }
		classDefinition.declareDefaultConstructor(a(PUBLIC));
		method.comment("Filter: %s", filter.toString());

		Variable positionVariable1 = position1;
		Variable positionVariable2 = position2;

		Scope scope = method.getScope();
		Variable wasNullVariable = scope.declareVariable(type(boolean.class),
				"wasNull");

		ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(
				callSiteBinder, fieldReferenceCompiler(callSiteBinder,
						positionVariable1, positionVariable2, wasNullVariable,
						offset), metadata.getFunctionRegistry());
		ByteCodeNode body = filter.accept(visitor,scope);

		// for (int index = 0; index < getInputChannels(filter).size(); index++)
		// {
		// method.getBody().append(context.getVariable("this").setField(
		// blockFields.get(index),
		// context.getVariable("channels")
		// .invoke("get", Object.class,
		// com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt(index))
		// .cast(type(List.class, com.facebook.presto.spi.block.Block.class))));
		// }
		LabelNode end = new LabelNode("end");

		method.getBody().comment("boolean wasNull = false;")
				.putVariable(wasNullVariable, false).append(body)
				.getVariable(wasNullVariable).ifFalseGoto(end)
				.pop(boolean.class).push(false).visitLabel(end).retBoolean();

	}

	private ByteCodeExpression getByteCodeExpresstion(Scope scope,
			int offset, int field) {
		Variable first = scope.getVariable("probeBlocks");
		Variable second = scope.getVariable("buildBlocks");
		Variable buildIndex = scope.getVariable("buildIndex");

		if (field < offset) {
			return first.invoke("get", Object.class,
					com.facebook.presto.byteCode.expression.ByteCodeExpressions
							.constantInt(field));
		} else {
			// return second.invoke("get", Object.class,
			// com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt(field-offset))
			// .cast(type(List.class,
			// com.facebook.presto.spi.block.Block.class)).invoke("get",
			// Object.class, buildIndex);
			// ImmutableList.copyOf(checkNotNull(parameters,
			// "parameters is null"))
			return second
					.invoke("get",
							Object.class,
							com.facebook.presto.byteCode.expression.ByteCodeExpressions
									.constantInt(field - offset))
					.cast(type(List.class,
							com.facebook.presto.spi.block.Block.class))
					.invoke("get", Object.class, buildIndex);
		}
	}

	private RowExpressionVisitor<Scope, ByteCodeNode> fieldReferenceCompiler(
			final CallSiteBinder callSiteBinder,
			final Variable positionVariable1, final Variable positionVariable2,
			final Variable wasNullVariable, final int offset) {
		return new RowExpressionVisitor<Scope, ByteCodeNode>() {
			@Override
			public ByteCodeNode visitInputReference(
					InputReferenceExpression node, Scope context) {
				int field = node.getField();
				Type type = node.getType();

				Class<?> javaType = type.getJavaType();

				// ByteCodeExpression bce=context.getVariable("channels")
				// .invoke("get", Object.class,
				// com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt(field))
				// //.cast(type(com.facebook.presto.spi.block.Block.class))
				// ;

				Block isNullCheck;
				if (field < offset) {
					isNullCheck = new Block()
							.setDescription(
									format("block_%d.get%s()", field, type))
							.append(getByteCodeExpresstion(context, offset,
									field))
							// .getVariable("block_" + field)
							.getVariable(positionVariable1)
							.invokeInterface(
									com.facebook.presto.spi.block.Block.class,
									"isNull", boolean.class, int.class);
				} else {
					isNullCheck = new Block()
							.setDescription(
									format("block_%d.get%s()", field, type))
							.append(getByteCodeExpresstion(context, offset,
									field))
							// .getVariable("block_" + field)
							.getVariable(positionVariable2)
							.invokeInterface(
									com.facebook.presto.spi.block.Block.class,
									"isNull", boolean.class, int.class);
				}

				Block isNull = new Block().putVariable(wasNullVariable,
						true).pushJavaDefault(javaType);

				String methodName = "get"
						+ Primitives.wrap(javaType).getSimpleName();

				Block isNotNull;
				if (field < offset) {
					isNotNull = new Block()
							.append(loadConstant(
									callSiteBinder.bind(type, Type.class)))
							.append(getByteCodeExpresstion(context, offset,
									field))
							// .getVariable("block_" + field)
							.getVariable(positionVariable1)
							.invokeInterface(Type.class, methodName, javaType,
									com.facebook.presto.spi.block.Block.class,
									int.class);
				} else {
					isNotNull = new Block()
							.append(loadConstant(
									callSiteBinder.bind(type, Type.class)))
							.append(getByteCodeExpresstion(context, offset,
									field))
							// .getVariable("block_" + field)
							.getVariable(positionVariable2)
							.invokeInterface(Type.class, methodName, javaType,
									com.facebook.presto.spi.block.Block.class,
									int.class);
				}

				//return new IfStatement(context, isNullCheck, isNull, isNotNull);
				IfStatement ifStatement = new IfStatement();
				ifStatement
						.condition()
						.append(isNullCheck);

				ifStatement.ifTrue().append(isNull);


				ifStatement
						.ifFalse()
						.append(isNotNull);

				return ifStatement;
			}

			@Override
			public ByteCodeNode visitCall(CallExpression call,
					Scope context) {
				throw new UnsupportedOperationException("not yet implemented");
			}

			@Override
			public ByteCodeNode visitConstant(ConstantExpression literal,
					Scope context) {
				throw new UnsupportedOperationException("not yet implemented");
			}
		};
	}
	
}
