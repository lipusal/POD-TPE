package ar.edu.itba.args;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.io.File;

/**
 * Validates that a file argument refers to a file that exists.
 */
public class FileExistsValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
        if(!new File(value).exists()) {
            throw new ParameterException(name + " \"" + value + "\" does not exist");
        }
    }
}
