package com.bb021949.query;

import org.immutables.value.Value;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Style used for all models
 */
@Retention(RetentionPolicy.CLASS) // Make it class retention for incremental compilation
@Value.Style(
        init = "set*", // Builder initialization methods will have 'set' prefix
        typeImmutable = "*", // No prefix or suffix for generated immutable type
        visibility = Value.Style.ImplementationVisibility.PUBLIC, // Generated class will be always public
        defaults = @Value.Immutable(copy = false)) // Disable copy methods by default
public @interface ImmutableStyle {
}
