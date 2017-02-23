package com.cloudcomputing;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sniper on 2017.02.23..
 */
public class TupleTextWritable implements WritableComparable<TupleTextWritable> {

    private String firstKey;
    private String secondKey;

    public TupleTextWritable() {
    }

    public void setFirstKey(String firstKey) {
        this.firstKey = firstKey;
    }

    public void setSecondKey(String secondKey) {
        this.secondKey = secondKey;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstKey = in.readUTF();
        secondKey = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(firstKey);
        out.writeUTF(secondKey);
    }

    @Override
    public int compareTo(TupleTextWritable other) {
        int firstKeyComparison = firstKey.compareTo(other.firstKey);
        if (firstKeyComparison != 0) {
            return firstKeyComparison;
        }
        return secondKey.compareTo(other.secondKey);
    }

    @Override
    public String toString() {
        return firstKey + ' ' + secondKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TupleTextWritable that = (TupleTextWritable) o;

        if (firstKey != null ? !firstKey.equals(that.firstKey) : that.firstKey != null) return false;
        return secondKey != null ? secondKey.equals(that.secondKey) : that.secondKey == null;
    }

    @Override
    public int hashCode() {
        int result = firstKey != null ? firstKey.hashCode() : 0;
        result = 31 * result + (secondKey != null ? secondKey.hashCode() : 0);
        return result;
    }
}
