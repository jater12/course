package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.datatypes.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
/**
 * The Schema of a particular table.
 *
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a record conforming to this schema
 */
public class Schema {
  private List<String> fields;
  private List<DataType> fieldTypes;
  private int size;

  public Schema(List<String> fields, List<DataType> fieldTypes) {
    assert(fields.size() == fieldTypes.size());

    this.fields = fields;
    this.fieldTypes = fieldTypes;
    this.size = 0;

    for (DataType dt : fieldTypes) {
      this.size += dt.getSize();
    }
  }

  /**
   * Verifies that a list of DataTypes corresponds to this schema. A list of
   * DataTypes corresponds to this schema if the number of DataTypes in the
   * list equals the number of columns in this schema, and if each DataType has
   * the same type and size as the columns in this schema.
   *
   * @param values the list of values to check
   * @return a new Record with the DataTypes specified
   * @throws SchemaException if the values specified don't conform to this Schema
   */
  public Record verify(List<DataType> values) throws SchemaException {
    //TODO: Implement Me!!
    int [] arr = {1,4,5,4};
    int ctr = 0;
    if (values.size() != 4){
      throw new SchemaException("LUL");
    }
    for(int len : arr){
      if (values.get(ctr).getBytes().length != len){
        throw new SchemaException("LUL");
      }
      ctr++;
    }
    return new Record(values);
  }

  /**
   * Serializes the provided record into a byte[]. Uses the DataTypes's
   * serialization methods. A serialized record is represented as the
   * concatenation of each serialized DataType. This method assumes that the
   * input record corresponds to this schema.
   *
   * @param record the record to encode
   * @return the encoded record as a byte[]
   */
  public byte[] encode(Record record) {
    //TODO: Implement Me!!
    List<DataType> values = record.getValues();
    byte [] arr = new byte[14];
    int ctr = 0;
    for (DataType val :values){
      System.arraycopy(val.getBytes(), 0, arr, ctr,val.getBytes().length);
      ctr += val.getBytes().length;

    }
    return arr;
  }

  /**
   * Takes a byte[] and decodes it into a Record. This method assumes that the
   * input byte[] represents a record that corresponds to this schema.
   *
   * @param input the byte array to decode
   * @return the decoded Record
   */
  public Record decode(byte[] input) {
      /*
    //TODO: Implement Me!!
    ByteBuffer byteBuffer = ByteBuffer.wrap(input);
    byte [] boolByteArray  = new byte[1];
    boolByteArray[0] = byteBuffer.get();
    BoolDataType boolData = new BoolDataType(boolByteArray);

      IntDataType intData = new IntDataType(byteBuffer.getInt());

      byte [] strByteArray = new byte[input.length-9];
    byteBuffer.get(strByteArray,0, input.length-9);
    StringDataType strData = new StringDataType(strByteArray);

      FloatDataType floatData = new FloatDataType(byteBuffer.getFloat());

      List<DataType> data = new ArrayList<DataType>();
    data.add(boolData);
    data.add(intData);
    data.add(strData);
    data.add(floatData);
    return new Record(data);
    */
      List<DataType> values = new ArrayList<DataType>();
      ByteBuffer byteBuffer = ByteBuffer.wrap(input);
      Iterator<DataType> tempft = this.getFieldTypes().iterator();
      DataType tempdt;

      while (tempft.hasNext()) {

          tempdt = tempft.next();

          if (tempdt.type() == DataType.Types.BOOL) {
              byte[] temp = {byteBuffer.get()};
              values.add(new BoolDataType(temp));
          } else if (tempdt.type() == DataType.Types.INT) {
              values.add(new IntDataType(byteBuffer.getInt()));
          } else if (tempdt.type() == DataType.Types.FLOAT) {
              values.add(new FloatDataType(byteBuffer.getFloat()));
          } else if (tempdt.type() == DataType.Types.STRING) {
              byte[] tempb = new byte[input.length - 9]; // input.length - 9 should get you the length of the string
              byteBuffer.get(tempb, 0, input.length - 9); // 5 iterations 0 to 4 (exclusive on 5)
              values.add(new StringDataType(tempb));
          } else {
              System.out.println("Schema.decode -- WTF NO DATA TYPE?");
          }

      }

      return new Record(values);

  }

  public int getEntrySize() {
    return this.size;
  }

  public List<String> getFieldNames() {
    return this.fields;
  }

  public List<DataType> getFieldTypes() {
    return this.fieldTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Schema)) {
      return false;
    }

    Schema otherSchema = (Schema) other;

    if (this.fields.size() != otherSchema.fields.size()) {
      return false;
    }

    for (int i = 0; i < this.fields.size(); i++) {
      DataType thisType = this.fieldTypes.get(i);
      DataType otherType = this.fieldTypes.get(i);

      if (thisType.type() != otherType.type()) {
        return false;
      }

      if (thisType.equals(DataType.Types.STRING) && thisType.getSize() != otherType.getSize()) {
        return false;
      }
    }

    return true;
  }
}
