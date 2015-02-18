package com.mobileum.range.presto;
public class Context<C> {
	private final boolean defaultRewrite;
	private final C context;

	public Context(C context, boolean defaultRewrite) {
		this.context = context;
		this.defaultRewrite = defaultRewrite;
	}

	public C get() {
		return context;
	}

	public boolean isDefaultRewrite() {
		return defaultRewrite;
	}
}