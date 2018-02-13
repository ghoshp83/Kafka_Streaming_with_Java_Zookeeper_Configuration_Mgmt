package com.pralay.cm.managed;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertiesTemplateValidator {
    private final List<EntryValidator> validators;

    public PropertiesTemplateValidator(final InputStream template) throws IOException {
        StringBuilder documentation = new StringBuilder();
        boolean mandatory = false;
        ImmutableList.Builder<EntryValidator> validatorBuilder = ImmutableList.builder();
        for (String line: IOUtils.readLines(template)) {
            if (documentation.length() == 0 && StringUtils.isBlank(line))
                continue;
            if (line.trim().startsWith("#") || line.trim().startsWith("!")) {
                if (documentation.length() != 0) {
                    documentation.append('\n');
                }
                documentation.append(line);
                if (line.matches("[ ]*![ ]*(mandatory|MANDATORY)[ ]*.*"))
                    mandatory = true;
            } else {
                List<String> keyValue = ImmutableList.copyOf(Splitter.on('=').trimResults().split(line));
                if (keyValue.size() > 1) {
                    String key = keyValue.get(0);
                    String value = keyValue.get(1);
                    boolean keyIsRegexp = key.matches(".*/.*/");
                    boolean valueIsRegexp = value.matches("/.*/");
                    if (keyIsRegexp || valueIsRegexp) {
                        Pattern keyPattern = keyIsRegexp ? Pattern.compile(key.substring(key.indexOf('/')+1, key.lastIndexOf('/'))) : null;
                        Pattern valuePattern = valueIsRegexp ? Pattern.compile(value.substring(value.indexOf('/')+1, value.lastIndexOf('/'))) : null;
                        if (documentation.length() != 0) {
                            documentation.append('\n');
                        }
                        documentation.append(line);
                        EntryValidator entryValidator = new EntryValidator(
                                keyIsRegexp ? key.substring(0, key.indexOf('/')) : key,
                                documentation.toString(),
                                keyPattern,
                                valuePattern
                        );
                        if (mandatory)
                            entryValidator.setMandatory(true);
                        validatorBuilder.add(entryValidator);
                    }
                    documentation.setLength(0);
                    mandatory = false;
                }
            }
        }
        validators = validatorBuilder.build();
    }


    public Map<String, String> validate(final Configuration configuration) {
        ImmutableMap.Builder<String, String> errorsBuilder = ImmutableMap.builder();
        for (EntryValidator validatorEntry: validators) {
            boolean mandatoryMissing = validatorEntry.isMandatory();
            for (String key: ImmutableList.copyOf((Iterator<String>)configuration.getKeys())) {
                if (!validatorEntry.isValid(key, configuration.getString(key))) {
                    errorsBuilder.put(key+"="+configuration.getString(key), validatorEntry.documentation);
                }
                if (mandatoryMissing && validatorEntry.fulfillsMandatory(key)) {
                    mandatoryMissing = false;
                }
            }
            if (mandatoryMissing) {
                errorsBuilder.put(validatorEntry.getKeyPrefix(), validatorEntry.documentation);
            }
        }

        // check mandatory
        return errorsBuilder.build();
    }

    private static class EntryValidator {
        private final String keyPrefix;

        private final String documentation;

        private final Pattern keyPattern;

        private final Pattern valuePattern;

        private boolean mandatory;

        public EntryValidator(final String keyPrefix, final String documentation, final Pattern keyPattern, final Pattern valuePattern) {
            this.keyPrefix = keyPrefix;
            this.documentation = documentation;
            this.keyPattern = keyPattern;
            this.valuePattern = valuePattern;
        }

        public void setMandatory(boolean mandatory) {
            this.mandatory = mandatory;
        }

        public boolean isMandatory() {
            return mandatory;
        }

        public String getDocumentation() {
            return documentation;
        }

        public Pattern getKeyPattern() {
            return keyPattern;
        }

        public Pattern getValuePattern() {
            return valuePattern;
        }

        public boolean shouldValidate(final String key) {
            return keyPrefix.equals(key) || (keyPattern != null && key.startsWith(keyPrefix));
        }

        public String getKeyPrefix() {
            return keyPrefix;
        }

        public boolean isValid(String key, String value) {
            if (keyPattern != null && key.startsWith(keyPrefix)) {
                Matcher m = keyPattern.matcher(key.substring(keyPrefix.length()));
                if (!m.matches())
                    return false;
            } else if (!keyPrefix.equals(key)) {
                return true;
            }

            if (valuePattern != null) {
                Matcher m = valuePattern.matcher(value);
                if (!m.matches())
                    return false;
            }
            return true;
        }

        public boolean fulfillsMandatory(String key) {
            if (keyPattern != null && key.startsWith(keyPrefix)) {
                return  keyPattern.matcher(key.substring(keyPrefix.length())).matches();
            } else
                return key.equals(keyPrefix);
        }
    }
}
