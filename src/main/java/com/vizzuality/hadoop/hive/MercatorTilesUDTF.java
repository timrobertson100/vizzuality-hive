package com.vizzuality.hadoop.hive;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import static com.vizzuality.hadoop.hive.MercatorProjectionUtil.TILE_SIZE;
import static com.vizzuality.hadoop.hive.MercatorProjectionUtil.isPlottable;
import static com.vizzuality.hadoop.hive.MercatorProjectionUtil.toNormalisedPixelCoords;

public class MercatorTilesUDTF extends GenericUDTF {

  private static final String[] FIELD_NAMES = {"zoom", "tile_x", "tile_y", "pixel_x", "pixel_y"};

  private final Object[] result = new Object[5];
  private final IntWritable zoomWritable = new IntWritable();
  private final IntWritable tileXWritable = new IntWritable();
  private final IntWritable tileYWritable = new IntWritable();
  private final IntWritable pixelXWritable = new IntWritable();
  private final IntWritable pixelYWritable = new IntWritable();

  private Converter latConverter;

  private Converter lngConverter;

  private Converter maxZoomConverter;

  private boolean firstRun = true;

  private int maxZoom = 23;

  @Override
  public StructObjectInspector initialize(StructObjectInspector argOI) throws UDFArgumentException {
    List<? extends StructField> inputFields = argOI.getAllStructFieldRefs();
    if (inputFields.size() < 3) {
      throw new UDFArgumentLengthException("mercator_tile() takes three arguments: latitude, longitude, max zoom (optional, default = 23)");
    }

    verifyArgumentType(inputFields.get(0).getFieldObjectInspector(), 1, "latitude");
    verifyArgumentType(inputFields.get(1).getFieldObjectInspector(), 2, "longitude");
    verifyArgumentType(inputFields.get(2).getFieldObjectInspector(), 3, "max_zoom");

    List<String> fieldNames = new ArrayList<>();
    List<ObjectInspector> fieldOIs = new ArrayList<>();
    for (String fieldName : FIELD_NAMES) {
      fieldNames.add(fieldName);
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    }
    result[0] = zoomWritable;
    result[1] = tileXWritable;
    result[2] = tileYWritable;
    result[3] = pixelXWritable;
    result[4] = pixelYWritable;

    latConverter = ObjectInspectorConverters.getConverter(inputFields.get(0).getFieldObjectInspector(),
                                                          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    lngConverter = ObjectInspectorConverters.getConverter(inputFields.get(1).getFieldObjectInspector(),
                                                          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    maxZoomConverter = ObjectInspectorConverters.getConverter(inputFields.get(2).getFieldObjectInspector(),
                                                              PrimitiveObjectInspectorFactory.javaIntObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  private void verifyArgumentType(ObjectInspector oi, int argumentNumber, String argumentName)
    throws UDFArgumentException {

    ObjectInspector.Category category = oi.getCategory();
    if (category != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(argumentNumber,
                                         "The "
                                         + GenericUDFUtils.getOrdinal(1)
                                         + " argument ("
                                         + argumentName
                                         + ")  is expected to be a "
                                         + ObjectInspector.Category.PRIMITIVE.toString().toLowerCase()
                                         + " type, but "
                                         + category.toString().toLowerCase()
                                         + " is found");
    }

    PrimitiveObjectInspector primitiveOI = (PrimitiveObjectInspector) oi;
    if (!FunctionRegistry.isNumericType(primitiveOI.getTypeInfo())) {
      throw new UDFArgumentTypeException(0,
                                         "The "
                                         + GenericUDFUtils.getOrdinal(1)
                                         + " argument ("
                                         + argumentName
                                         + "} is expected to be a "
                                         + "numeric type, but "
                                         + primitiveOI.getTypeName()
                                         + " is found");
    }
  }

  @Override
  public void process(Object[] args) throws HiveException {
    if (args[0] == null || args[1] == null) {
      return;
    }

    if (firstRun) {
      if (args.length == 3) {
        maxZoom = (int) maxZoomConverter.convert(args[2]);
      }
      firstRun = false;
    }

    Object input = latConverter.convert(args[0]);
    if (input == null) {
      return;
    }
    double latitude = (double) input;

    input = lngConverter.convert(args[1]);
    if (input == null) {
      return;
    }
    double longitude = (double) input;
    processRow(latitude, longitude);
  }

  public void processRow(double latitude, double longitude) throws HiveException {
    if (isPlottable(latitude, longitude)) {
      Point2D normalizedPixels = toNormalisedPixelCoords(latitude, longitude);

      for (int z= 0; z<=maxZoom; z++) {
        int scale = 1 << z;

        // truncating to int removes the fractional pixel offset
        tileXWritable.set((int) (normalizedPixels.getX() * scale));
        tileYWritable.set((int) (normalizedPixels.getY() * scale));

        double x = normalizedPixels.getX();
        x *= scale * TILE_SIZE;
        pixelXWritable.set((int) x % TILE_SIZE);

        double y = normalizedPixels.getY();
        y *= scale * TILE_SIZE;
        pixelYWritable.set((int) y - TILE_SIZE * tileYWritable.get());

        zoomWritable.set(z);
        forward(result);
      }
    }
  }

  @Override
  public void close() throws HiveException {
  }
}
