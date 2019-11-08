package file;

import id.Id;

import java.io.Serializable;

public class FieldId implements Id, Serializable {
    private final String id;

    public FieldId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object otherObject) {
        if(this == otherObject) {
            return true;
        }
        if(otherObject == null) {
            return false;
        }
        if(getClass() != otherObject.getClass()) {
            return false;
        }
        String other = ((FieldId) otherObject).getId();
        return id.compareTo(other) == 0;
    }
}
