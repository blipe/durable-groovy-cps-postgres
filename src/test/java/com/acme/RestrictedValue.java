package com.acme;

import java.io.Serializable;

public record RestrictedValue(String value) implements Serializable {}
