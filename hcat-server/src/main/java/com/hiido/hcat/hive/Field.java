package com.hiido.hcat.hive;

public final class Field {
    private String name;
    private String type;

    public Field(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public Field() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("name[%s],type[%s]", name, type);
    }

}
