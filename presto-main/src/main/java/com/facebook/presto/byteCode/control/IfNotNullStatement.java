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
package com.facebook.presto.byteCode.control;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * 
 * @author dilip kasana
 * @Date 01 june 2015
 */
public class IfNotNullStatement implements FlowControl {
	private final String comment;
	private final Block condition;
	private final Block ifNotNull;
	private final Block ifNull;

	private final LabelNode ifNotNullLabel = new LabelNode("false");
	private final LabelNode outLabel = new LabelNode("out");

	public IfNotNullStatement(Block condition, Block ifTrue, Block ifFalse) {
		this(null, condition, ifTrue, ifFalse);
	}

	public IfNotNullStatement(String comment, Block condition, Block ifTrue,
			Block ifFalse) {
		this.comment = comment;
		this.condition = condition;
		this.ifNotNull = ifTrue;
		this.ifNull = ifFalse;
	}

	@Override
	public String getComment() {
		return comment;
	}

	public ByteCodeNode getCondition() {
		return condition;
	}

	public ByteCodeNode getIfTrue() {
		return ifNotNull;
	}

	public ByteCodeNode getIfFalse() {
		return ifNull;
	}

	@Override
	public void accept(MethodVisitor visitor,
			MethodGenerationContext generationContext) {

		checkState(!condition.isEmpty(),
				"IfStatement does not have a condition set");
		checkState(!ifNull.isEmpty() || !ifNotNull.isEmpty(),
				"IfStatement does not have a true or false block set");
		/*
		 * block.dup(methodType.returnType()) .ifNotNullGoto(notNull)
		 * .putVariable("wasNull", true)
		 * .comment("swap boxed null with unboxed default")
		 * .pop(methodType.returnType()) .pushJavaDefault(unboxedReturnType)
		 * .gotoLabel(end) .visitLabel(notNull) .append(unboxPrimitive(context,
		 * unboxedReturnType));
		 */
		Block block = new Block();

		block.append(condition).ifNotNullGoto(ifNotNullLabel).append(ifNull);

		// IfNotNull
		if (ifNotNull != null) {
			block.gotoLabel(outLabel).visitLabel(ifNotNullLabel)
					.append(ifNotNull).visitLabel(outLabel);
		} else {
			block.visitLabel(ifNotNullLabel);
		}

		block.accept(visitor, generationContext);
	}

	@Override
	public List<ByteCodeNode> getChildNodes() {
		if (ifNull == null) {
			return ImmutableList.of(condition, ifNotNull);
		} else {
			return ImmutableList.of(condition, ifNotNull, ifNull);
		}
	}

	@Override
	public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor) {
		return visitor.visitIfNotNull(parent, this);
	}
}
