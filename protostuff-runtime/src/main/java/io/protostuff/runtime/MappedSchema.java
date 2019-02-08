//========================================================================
package io.protostuff.runtime;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Pipe;
import io.protostuff.Schema;
import io.protostuff.Tag;
import io.protostuff.WireFormat.FieldType;

/**
 * Base class for schemas that maps fields by number and name. For fast initialization, the last field number is
 * provided in the constructor.
 * 
 * @author David Yu
 * @created Nov 10, 2009
 */
public abstract class MappedSchema<T> implements Schema<T> {

    protected final Class<T> typeClass;

    protected final Field<T>[] fields, fieldsByNumber;

    protected final Map<String, Field<T>> fieldsByName;

    protected final Pipe.Schema<T> pipeSchema;

    @SuppressWarnings(value = { "unchecked" })
    public MappedSchema(Class<T> typeClass, Field<T>[] fields, int lastFieldNumber) {
        if (fields.length == 0) {
            throw new IllegalStateException("At least one field is required.");
        }
        this.typeClass = typeClass;
        this.fields = fields;
        fieldsByName = new HashMap<>();
        fieldsByNumber = (Field<T>[]) new Field<?>[lastFieldNumber + 1];
        for (Field<T> f : fields) {
            Field<T> last = this.fieldsByName.put(f.name, f);
            if (last != null) {
                throw new IllegalStateException(last + " and " + f + " cannot have the same name.");
            }
            if (fieldsByNumber[f.number] != null) {
                throw new IllegalStateException(fieldsByNumber[f.number] + " and " + f + " cannot have the same number.");
            }
            fieldsByNumber[f.number] = f;
        }
        pipeSchema = new RuntimePipeSchema<>(this, fieldsByNumber);
    }

    @SuppressWarnings(value = { "unchecked" })
    public MappedSchema(Class<T> typeClass, Collection<Field<T>> fields, int lastFieldNumber) {
        this.typeClass = typeClass;
        fieldsByName = new HashMap<>();
        fieldsByNumber = (Field<T>[]) new Field<?>[lastFieldNumber + 1];
        for (Field<T> f : fields) {
            Field<T> last = this.fieldsByName.put(f.name, f);
            if (last != null) {
                throw new IllegalStateException(last + " and " + f + " cannot have the same name.");
            }
            if (fieldsByNumber[f.number] != null) {
                throw new IllegalStateException(fieldsByNumber[f.number] + " and " + f + " cannot have the same number.");
            }
            fieldsByNumber[f.number] = f;
        }
        this.fields = (Field<T>[]) new Field<?>[fields.size()];
        for (int i = 1, j = 0; i < fieldsByNumber.length; i++) {
            if (fieldsByNumber[i] != null) {
                this.fields[j++] = fieldsByNumber[i];
            }
        }
        pipeSchema = new RuntimePipeSchema<>(this, fieldsByNumber);
    }

    @SuppressWarnings(value = { "unchecked" })
    public MappedSchema(Class<T> typeClass, Map<String, Field<T>> fieldsByName, int lastFieldNumber) {
        this.typeClass = typeClass;
        this.fieldsByName = fieldsByName;
        Collection<Field<T>> fields = fieldsByName.values();
        fieldsByNumber = (Field<T>[]) new Field<?>[lastFieldNumber + 1];
        for (Field<T> f : fields) {
            if (fieldsByNumber[f.number] != null) {
                throw new IllegalStateException(fieldsByNumber[f.number] + " and " + f + " cannot have the same number.");
            }
            fieldsByNumber[f.number] = f;
        }
        this.fields = (Field<T>[]) new Field<?>[fields.size()];
        for (int i = 1, j = 0; i < fieldsByNumber.length; i++) {
            if (fieldsByNumber[i] != null) {
                this.fields[j++] = fieldsByNumber[i];
            }
        }
        pipeSchema = new RuntimePipeSchema<>(this, fieldsByNumber);
    }

    /**
     * Returns the message's total number of fields.
     */
    public int getFieldCount() {
        return fields.length;
    }

    @Override
    public Class<T> typeClass() {
        return typeClass;
    }

    @Override
    public String messageName() {
        return typeClass.getSimpleName();
    }

    @Override
    public String messageFullName() {
        return typeClass.getName();
    }

    @Override
    public String getFieldName(int number) {
        final Field<T> field = number < fieldsByNumber.length ? fieldsByNumber[number] : null;
        return field == null ? null : field.name;
    }

    @Override
    public int getFieldNumber(String name) {
        final Field<T> field = fieldsByName.get(name);
        return field == null ? 0 : field.number;
    }

    @Override
    public final void mergeFrom(Input input, T message) throws IOException {
        for (int number = input.readFieldNumber(this); number != 0; number = input.readFieldNumber(this)) {
            final Field<T> field = number < fieldsByNumber.length ? fieldsByNumber[number] : null;
            if (field == null) {
                input.handleUnknownField(number, this);
            } else {
                field.mergeFrom(input, message);
            }
        }
    }

    @Override
    public final void writeTo(Output output, T message) throws IOException {
        for (Field<T> f : fields) {
            f.writeTo(output, message);
        }
    }

    /**
     * Returns the pipe schema linked to this.
     */
    public Pipe.Schema<T> getPipeSchema() {
        return pipeSchema;
    }

    /**
     * Represents a field of a message/pojo.
     */
    public abstract static class Field<T> {

        public final FieldType type;

        public final int number;

        public final String name;

        public final boolean repeated;

        public final int groupFilter;

        public Field(FieldType type, int number, String name, boolean repeated, Tag tag) {
            this.type = type;
            this.number = number;
            this.name = name;
            this.repeated = repeated;
            this.groupFilter = tag == null ? 0 : tag.groupFilter();
        }

        public Field(FieldType type, int number, String name, Tag tag) {
            this(type, number, name, false, tag);
        }

        /**
         * Writes the value of a field to the {@code output}.
         */
        protected abstract void writeTo(Output output, T message) throws IOException;

        /**
         * Reads the field value into the {@code message}.
         */
        protected abstract void mergeFrom(Input input, T message) throws IOException;

        /**
         * Transfer the input field to the output field.
         */
        protected abstract void transfer(Pipe pipe, Input input, Output output, boolean repeated) throws IOException;
    }
}
