package ar.edu.itba.args;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility class with methods used for argument parsing.
 */
public class Util {

    /**
     * Capture known system properties and convert them to regular program arguments, as recognized by JCommander.
     *
     * @param properties The properties to read.
     * @param knownProperties The known properties to read from {@code properties}
     * @return The present recognized properties, in regular program argument format.
     */
    public static List<String> propertiesToArgs(Properties properties, Collection<String> knownProperties) {
        return knownProperties.stream()
                .map(propertyName -> {
                    Optional<String> value = Optional.ofNullable(properties.getProperty(propertyName));
                    // If present, transform "-Dkey=value" to "-key", "value". Else null
                    return value.map(presentValue -> new String[] {"-" + propertyName, presentValue}).orElse(null);
                })
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }
}
