// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.core.attribute;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectReader;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectWriter;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.KryoException;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.core.attribute.JtsGeoshapeHelper;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.diskstorage.util.ReadArrayBuffer;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.ShapeFactory.LineStringBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.MultiShapeBuilder;
import org.locationtech.spatial4j.shape.SpatialRelation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A generic representation of a geographic shape, which can either be a single point,
 * circle, box, line or polygon. Use {@link #getType()} to determine the type of shape of a particular Geoshape object.
 * Use the static constructor methods to create the desired geoshape.
 */

public class Geoshape {

    private static final String FIELD_LABEL = "geometry";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_COORDINATES = "coordinates";

    private static final JtsGeoshapeHelper HELPER = new JtsGeoshapeHelper();

    private static final ObjectReader mapReader;
    private static final ObjectWriter mapWriter;

    static {
        final ObjectMapper mapper = new ObjectMapper();
        mapReader = mapper.readerWithView(LinkedHashMap.class).forType(LinkedHashMap.class);
        mapWriter = mapper.writerWithView(Map.class);
    }

    /**
     * The Type of a shape: a point, box, circle, line or polygon.
     */
    public enum Type {
        POINT("Point"),
        BOX("Box"),
        CIRCLE("Circle"),
        LINE("Line"),
        POLYGON("Polygon"),
        MULTIPOINT("MultiPoint"),
        MULTILINESTRING("MultiLineString"),
        MULTIPOLYGON("MultiPolygon"),
        GEOMETRYCOLLECTION("GeometryCollection");

        private final String gsonName;

        Type(String gsonName) {
            this.gsonName = gsonName;
        }

        public boolean gsonEquals(String otherGson) {
            return gsonName.equals(otherGson);
        }

        public static Type fromGson(String gsonShape) {
            return Type.valueOf(gsonShape.toUpperCase());
        }

        @Override
        public String toString() {
            return gsonName;
        }
    }

    private final Shape shape;

    protected Geoshape(Shape shape) {
        this.shape = Preconditions.checkNotNull(shape, "Invalid shape (null)");
    }

    @Override
    public int hashCode() {
        return shape.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        org.janusgraph.core.attribute.Geoshape oth = (org.janusgraph.core.attribute.Geoshape) other;
        return shape.equals(oth.shape);
    }

    /**
     * Returns the WKT representation of the shape.
     */
    @Override
    public String toString() {
        return HELPER.getWktWriter().toString(shape);
    }

    /**
     * Returns the GeoJSON representation of the shape.
     */
    private String toGeoJson() {
        return GeoshapeGsonSerializerV2d0.toGeoJson(this);
    }

    public Map<String, Object> toMap() throws IOException {
        return mapReader.readValue(toGeoJson());
    }

    /**
     * Returns the underlying {@link Shape}.
     */
    public Shape getShape() {
        return shape;
    }

    /**
     * Returns the {@link Type} of this geoshape.
     */
    public Type getType() {
        return HELPER.getType(shape);
    }

    /**
     * Returns the number of points comprising this geoshape. A point and circle have only one point (center of circle),
     * a box has two points (the south-west and north-east corners). Lines and polygons have a variable number of points.
     */
    public int size() {
        return HELPER.size(shape);
    }

    /**
     * Returns the point at the given position. The position must be smaller than {@link #size()}.
     */
    public Point getPoint(int position) {
        return HELPER.getPoint(this, position);
    }

    /**
     * Returns the singleton point of this shape. Only applicable for point and circle shapes.
     */
    public Point getPoint() {
        Preconditions.checkArgument(getType() == Type.POINT || getType() == Type.CIRCLE, "Shape does not have a single point");
        return new Point(shape.getCenter().getY(), shape.getCenter().getX());
    }

    /**
     * Returns the radius in kilometers of this circle. Only applicable to circle shapes.
     */
    public double getRadius() {
        Preconditions.checkArgument(getType() == Type.CIRCLE, "This shape is not a circle");
        double radiusInDeg = ((Circle) shape).getRadius();
        return DistanceUtils.degrees2Dist(radiusInDeg, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    }

    private SpatialRelation getSpatialRelation(org.janusgraph.core.attribute.Geoshape other) {
        Preconditions.checkNotNull(other);
        return shape.relate(other.shape);
    }

    /**
     * Whether this geometry has any points in common with the given geometry.
     */
    public boolean intersect(org.janusgraph.core.attribute.Geoshape other) {
        SpatialRelation r = getSpatialRelation(other);
        return r == SpatialRelation.INTERSECTS || r == SpatialRelation.CONTAINS || r == SpatialRelation.WITHIN;
    }

    /**
     * Whether this geometry is within the given geometry.
     */
    public boolean within(org.janusgraph.core.attribute.Geoshape outer) {
        return getSpatialRelation(outer) == SpatialRelation.WITHIN;
    }

    /**
     * Whether this geometry contains the given geometry.
     */
    public boolean contains(org.janusgraph.core.attribute.Geoshape outer) {
        return getSpatialRelation(outer) == SpatialRelation.CONTAINS;
    }

    /**
     * Whether this geometry has no points in common with the given geometry.
     */
    public boolean disjoint(org.janusgraph.core.attribute.Geoshape other) {
        return getSpatialRelation(other) == SpatialRelation.DISJOINT;
    }


    /**
     * Constructs a point from its latitude and longitude information
     */
    public static org.janusgraph.core.attribute.Geoshape point(double latitude, double longitude) {
        Preconditions.checkArgument(isValidCoordinate(latitude, longitude), "Invalid coordinate provided");
        return new org.janusgraph.core.attribute.Geoshape(getShapeFactory().pointXY(longitude, latitude));
    }

    /**
     * Constructs a circle from a given center point and a radius in kilometer
     */
    public static org.janusgraph.core.attribute.Geoshape circle(double latitude, double longitude, double radiusInKM) {
        Preconditions.checkArgument(isValidCoordinate(latitude, longitude), "Invalid coordinate provided");
        Preconditions.checkArgument(radiusInKM > 0, "Invalid radius provided [%s]", radiusInKM);
        return new org.janusgraph.core.attribute.Geoshape(getShapeFactory().circle(longitude, latitude, DistanceUtils.dist2Degrees(radiusInKM, DistanceUtils.EARTH_MEAN_RADIUS_KM)));
    }

    /**
     * Constructs a new box shape which is identified by its south-west and north-east corner points
     */
    public static org.janusgraph.core.attribute.Geoshape box(double southWestLatitude, double southWestLongitude,
                                                             final double northEastLatitude, double northEastLongitude) {
        Preconditions.checkArgument(isValidCoordinate(southWestLatitude, southWestLongitude), "Invalid south-west coordinate provided");
        Preconditions.checkArgument(isValidCoordinate(northEastLatitude, northEastLongitude), "Invalid north-east coordinate provided");
        return new org.janusgraph.core.attribute.Geoshape(getShapeFactory().rect(southWestLongitude, northEastLongitude, southWestLatitude, northEastLatitude));
    }

    /**
     * Constructs a line from list of coordinates
     *
     * @param coordinates Coordinate (lon,lat) pairs
     */
    public static org.janusgraph.core.attribute.Geoshape line(List<double[]> coordinates) {
        Preconditions.checkArgument(coordinates.size() >= 2, "Too few coordinate pairs provided");
        final LineStringBuilder builder = getShapeFactory().lineString();
        for (double[] coordinate : coordinates) {
            Preconditions.checkArgument(isValidCoordinate(coordinate[1], coordinate[0]), "Invalid coordinate provided");
            builder.pointXY(coordinate[0], coordinate[1]);
        }
        return new org.janusgraph.core.attribute.Geoshape(builder.build());
    }

    /**
     * Constructs a polygon from list of coordinates
     *
     * @param coordinates Coordinate (lon,lat) pairs
     */
    public static org.janusgraph.core.attribute.Geoshape polygon(List<double[]> coordinates) {
        return HELPER.polygon(coordinates);
    }

    /**
     * Constructs a Geoshape from a spatial4j {@link Shape}.
     */
    public static org.janusgraph.core.attribute.Geoshape geoshape(Shape shape) {
        return new org.janusgraph.core.attribute.Geoshape(shape);
    }

    /**
     * Create Geoshape from WKT representation.
     */
    public static org.janusgraph.core.attribute.Geoshape fromWkt(String wkt) throws ParseException {
        return new org.janusgraph.core.attribute.Geoshape(HELPER.getWktReader().parse(wkt));
    }

    /**
     * Whether the given coordinates mark a point on earth.
     */
    static boolean isValidCoordinate(double latitude, double longitude) {
        return latitude >= -90.0 && latitude <= 90.0 && longitude >= -180.0 && longitude <= 180.0;
    }

    public static SpatialContext getSpatialContext() {
        return HELPER.getContext();
    }

    public static ShapeFactory getShapeFactory() {
        return getSpatialContext().getShapeFactory();
    }

    public static MultiShapeBuilder<Shape> getGeometryCollectionBuilder() {
        return getShapeFactory().multiShape(Shape.class);
    }

    /**
     * A single point representation. A point is identified by its coordinate on the earth sphere using the spherical
     * system of latitudes and longitudes.
     */
    public static final class Point {

        private final double longitude;
        private final double latitude;

        /**
         * Constructs a point with the given latitude and longitude
         *
         * @param latitude  Between -90 and 90 degrees
         * @param longitude Between -180 and 180 degrees
         */
        Point(double latitude, double longitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }

        /**
         * Longitude of this point
         */
        public double getLongitude() {
            return longitude;
        }

        /**
         * Latitude of this point
         */
        public double getLatitude() {
            return latitude;
        }

        private org.locationtech.spatial4j.shape.Point getSpatial4jPoint() {
            return getShapeFactory().pointXY(longitude, latitude);
        }

        /**
         * Returns the distance to another point in kilometers
         */
        public double distance(Point other) {
            return DistanceUtils.degrees2Dist(HELPER.getContext().getDistCalc().distance(getSpatial4jPoint(), other.getSpatial4jPoint()), DistanceUtils.EARTH_MEAN_RADIUS_KM);
        }

    }

    /**
     * Geoshape attribute serializer for JanusGraph.
     *
 */
    public static class GeoshapeSerializer implements AttributeSerializer<org.janusgraph.core.attribute.Geoshape> {

        @Override
        public void verifyAttribute(org.janusgraph.core.attribute.Geoshape value) {
            //All values of Geoshape are valid
        }

        @Override
        public org.janusgraph.core.attribute.Geoshape convert(Object value) {
            if (value instanceof Map) {
                return convertGeoJson(value);
            }

            if (value instanceof Collection) {
                value = convertCollection((Collection<Object>) value);
            }

            if (value.getClass().isArray() && (value.getClass().getComponentType().isPrimitive() || Number.class.isAssignableFrom(value.getClass().getComponentType()))) {
                int len = Array.getLength(value);
                double[] arr = new double[len];
                for (int i = 0; i < len; i++) arr[i] = ((Number) Array.get(value, i)).doubleValue();
                switch (len) {
                    case 2:
                        return point(arr[0], arr[1]);
                    case 3:
                        return circle(arr[0], arr[1], arr[2]);
                    case 4:
                        return box(arr[0], arr[1], arr[2], arr[3]);
                    default:
                        throw new IllegalArgumentException("Expected 2-4 coordinates to create Geoshape, but given: " + value);
                }
            } else if (value instanceof String) {
                String[] components = null;
                for (String delimiter : new String[]{",", ";"}) {
                    components = ((String) value).split(delimiter);
                    if (components.length >= 2 && components.length <= 4) break;
                    else components = null;
                }
                Preconditions.checkNotNull(components, "Could not parse coordinates from string: %s", value);
                double[] coordinates = new double[components.length];
                try {
                    for (int i = 0; i < components.length; i++) {
                        coordinates[i] = Double.parseDouble(components[i]);
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Could not parse coordinates from string: " + value, e);
                }
                return convert(coordinates);
            } else return null;
        }


        private double[] convertCollection(Collection<Object> c) {

            List<Double> numbers = c.stream().map(o -> {
                if (!(o instanceof Number)) {
                    throw new IllegalArgumentException("Collections may only contain numbers to create a Geoshape");
                }
                return ((Number) o).doubleValue();
            }).collect(Collectors.toList());
            return Doubles.toArray(numbers);
        }

        private org.janusgraph.core.attribute.Geoshape convertGeoJson(Object value) {
            //Note that geoJson is long,lat
            try {
                Map<String, Object> map = (Map) value;
                String type = (String) map.get("type");
                if ("Feature".equals(type)) {
                    Map<String, Object> geometry = (Map) map.get("geometry");
                    return convertGeometry(geometry);
                } else {
                    return convertGeometry(map);
                }
            } catch (ClassCastException | IOException | ParseException e) {
                throw new IllegalArgumentException("GeoJSON was unparsable");
            }
        }

        private org.janusgraph.core.attribute.Geoshape convertGeometry(Map<String, Object> geometry) throws IOException, ParseException {
            String type = (String) geometry.get("type");
            List<Object> coordinates = (List) geometry.get(FIELD_COORDINATES);

            switch (type) {
                case "Point": {
                    double[] parsedCoordinates = convertCollection(coordinates);
                    return point(parsedCoordinates[1], parsedCoordinates[0]);
                }
                case "Circle": {
                    Number radius = (Number) geometry.get("radius");
                    if (radius == null) {
                        throw new IllegalArgumentException("GeoJSON circles require a radius");
                    }
                    double[] parsedCoordinates = convertCollection(coordinates);
                    return circle(parsedCoordinates[1], parsedCoordinates[0], radius.doubleValue());
                }
                case "Polygon":
                    // check whether this is a box
                    if (coordinates.size() == 4) {
                        double[] p0 = convertCollection((Collection) coordinates.get(0));
                        double[] p1 = convertCollection((Collection) coordinates.get(1));
                        double[] p2 = convertCollection((Collection) coordinates.get(2));
                        double[] p3 = convertCollection((Collection) coordinates.get(3));

                        //This may be a clockwise or counterclockwise polygon, we have to verify that it is a box
                        if ((p0[0] == p1[0] && p1[1] == p2[1] && p2[0] == p3[0] && p3[1] == p0[1] && p3[0] != p0[0]) ||
                                (p0[1] == p1[1] && p1[0] == p2[0] && p2[1] == p3[1] && p3[0] == p0[0] && p3[1] != p0[1])) {
                            return box(min(p0[1], p1[1], p2[1], p3[1]), min(p0[0], p1[0], p2[0], p3[0]), max(p0[1], p1[1], p2[1], p3[1]), max(p0[0], p1[0], p2[0], p3[0]));
                        }
                    }
                    break;
            }

            String json = mapWriter.writeValueAsString(geometry);
            return new org.janusgraph.core.attribute.Geoshape(HELPER.getGeojsonReader().read(new StringReader(json)));
        }

        private double min(double... numbers) {
            return Arrays.stream(numbers).min().getAsDouble();
        }

        private double max(double... numbers) {
            return Arrays.stream(numbers).max().getAsDouble();
        }


        @Override
        public org.janusgraph.core.attribute.Geoshape read(ScanBuffer buffer) {
            long l = VariableLong.readPositive(buffer);
            int length = (int) l;
            int position = ((ReadArrayBuffer) buffer).getPosition();
            InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(length));
            try {
                return GeoshapeBinarySerializer.read(inputStream);
            } catch (IOException e) {
                // retry using legacy point deserialization
                try {
                    ((ReadArrayBuffer) buffer).movePositionTo(position);
                    final float lat = buffer.getFloat();
                    final float lon = buffer.getFloat();
                    return point(lat, lon);
                } catch (Exception ignored) {
                }
                // throw original exception
                throw new RuntimeException("I/O exception reading geoshape", e);
            }
        }

        @Override
        public void write(WriteBuffer buffer, org.janusgraph.core.attribute.Geoshape attribute) {
            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                GeoshapeBinarySerializer.write(outputStream, attribute);
                byte[] bytes = outputStream.toByteArray();
                VariableLong.writePositive(buffer, bytes.length);
                buffer.putBytes(bytes);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception writing geoshape", e);
            }
        }
    }

    /**
     * Geoshape serializer for TinkerPop's Gryo.
     */
    public static class GeoShapeGryoSerializer extends Serializer<org.janusgraph.core.attribute.Geoshape> {
        @Override
        public void write(Kryo kryo, Output output, org.janusgraph.core.attribute.Geoshape geoshape) {
            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                GeoshapeBinarySerializer.write(outputStream, geoshape);
                byte[] bytes = outputStream.toByteArray();
                output.writeLong(bytes.length);
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception writing geoshape", e);
            }
        }

        @Override
        public org.janusgraph.core.attribute.Geoshape read(Kryo kryo, Input input, Class<org.janusgraph.core.attribute.Geoshape> aClass) {
            long l = input.readLong();
            int length = (int) l;
            try {
                final InputStream inputStream = new ByteArrayInputStream(input.readBytes(length));
                return GeoshapeBinarySerializer.read(inputStream);
            } catch (IOException | KryoException e) {
                // retry using legacy point deserialization
                try {
                    input.setPosition(0);
                    input.readLong();
                    float lat = input.readFloat();
                    float lon = input.readFloat();
                    return point(lat, lon);
                } catch (KryoException ignored) {
                }
                // throw original exception
                throw new RuntimeException("I/O exception reading geoshape", e);
            }
        }
    }

    /**
     * Geoshape serializer for GraphSON 1.0 supporting writing GeoJSON (http://geojson.org/).
     */
    public static class GeoshapeGsonSerializerV1d0 extends StdSerializer<org.janusgraph.core.attribute.Geoshape> {

        public GeoshapeGsonSerializerV1d0() {
            super(org.janusgraph.core.attribute.Geoshape.class);
        }

        @Override
        public void serialize(org.janusgraph.core.attribute.Geoshape value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            switch (value.getType()) {
                case POINT:
                    jgen.writeStartObject();
                    jgen.writeFieldName(FIELD_TYPE);
                    jgen.writeString(Type.POINT.toString());
                    jgen.writeFieldName(FIELD_COORDINATES);
                    jgen.writeStartArray();
                    jgen.writeNumber(value.getPoint().getLongitude());
                    jgen.writeNumber(value.getPoint().getLatitude());
                    jgen.writeEndArray();
                    jgen.writeEndObject();
                    break;
                default:
                    jgen.writeRawValue(toGeoJson(value));
                    break;
            }
        }

        @Override
        public void serializeWithType(org.janusgraph.core.attribute.Geoshape geoshape, JsonGenerator jgen, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
            jgen.writeStartObject();
            if (typeSerializer != null) jgen.writeStringField(GraphSONTokens.CLASS, org.janusgraph.core.attribute.Geoshape.class.getName());
            String geojson = toGeoJson(geoshape);
            Map json = mapReader.readValue(geojson);
            if (geoshape.getType() == Type.POINT) {
                final double[] coords = ((List<Number>) json.get(FIELD_COORDINATES)).stream().map(Number::doubleValue).mapToDouble(i -> i).toArray();
                GraphSONUtil.writeWithType(FIELD_COORDINATES, coords, jgen, serializerProvider, typeSerializer);
            } else {
                GraphSONUtil.writeWithType(FIELD_LABEL, json, jgen, serializerProvider, typeSerializer);
            }
            jgen.writeEndObject();
        }

        static String toGeoJson(org.janusgraph.core.attribute.Geoshape geoshape) {
            return HELPER.getGeojsonWriter().toString(geoshape.shape);
        }
    }

    /**
     * Geoshape deserializer for GraphSON 1.0 supporting reading from GeoJSON (http://geojson.org/).
     */
    public static class GeoshapeGsonDeserializerV1d0 extends StdDeserializer<org.janusgraph.core.attribute.Geoshape> {

        public GeoshapeGsonDeserializerV1d0() {
            super(org.janusgraph.core.attribute.Geoshape.class);
        }

        @Override
        public org.janusgraph.core.attribute.Geoshape deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            jsonParser.nextToken();
            if (jsonParser.getCurrentName().equals(FIELD_COORDINATES)) {
                double[] f = jsonParser.readValueAs(double[].class);
                jsonParser.nextToken();
                return org.janusgraph.core.attribute.Geoshape.point(f[1], f[0]);
            } else {
                try {
                    HashMap map = jsonParser.readValueAs(LinkedHashMap.class);
                    jsonParser.nextToken();
                    String json = mapWriter.writeValueAsString(map);
                    return new org.janusgraph.core.attribute.Geoshape(HELPER.getGeojsonReader().read(new StringReader(json)));
                } catch (ParseException e) {
                    throw new IOException("Unable to read and parse geojson", e);
                }
            }
        }
    }

    /**
     * Geoshape serializer for GraphSON 2.0 supporting writing GeoJSON (http://geojson.org/).
     */
    public static class GeoshapeGsonSerializerV2d0 extends GeoshapeGsonSerializerV1d0 {

        public void serializeWithType(org.janusgraph.core.attribute.Geoshape geoshape, JsonGenerator jgen, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
            jgen.writeStartObject();
            if (typeSerializer != null) jgen.writeStringField(GraphSONTokens.VALUETYPE, "janusgraph:Geoshape");
            jgen.writeFieldName(GraphSONTokens.VALUEPROP);
            GraphSONUtil.writeStartObject(geoshape, jgen, typeSerializer);
            final Map json = mapReader.readValue(toGeoJson(geoshape));
            if (geoshape.getType() == Type.POINT) {
                final double[] coordinates = ((List<Number>) json.get(FIELD_COORDINATES)).stream().mapToDouble(Number::doubleValue).toArray();
                GraphSONUtil.writeWithType(FIELD_COORDINATES, coordinates, jgen, serializerProvider, typeSerializer);
            } else {
                GraphSONUtil.writeWithType(FIELD_LABEL, json, jgen, serializerProvider, typeSerializer);
            }
            GraphSONUtil.writeEndObject(geoshape, jgen, typeSerializer);
            jgen.writeEndObject();
        }

        public static String toGeoJson(org.janusgraph.core.attribute.Geoshape geoshape) {
            return HELPER.getGeojsonWriter().toString(geoshape.shape);
        }

    }

    /**
     * Geoshape deserializer for GraphSON 2.0 supporting reading from GeoJSON (http://geojson.org/).
     */
    public static class GeoshapeGsonDeserializerV2d0 extends AbstractObjectDeserializer<org.janusgraph.core.attribute.Geoshape> {

        public GeoshapeGsonDeserializerV2d0() {
            super(org.janusgraph.core.attribute.Geoshape.class);
        }

        @Override
        public org.janusgraph.core.attribute.Geoshape createObject(Map<String, Object> data) {
            org.janusgraph.core.attribute.Geoshape shape;
            if (data.containsKey(FIELD_COORDINATES) && data.get(FIELD_COORDINATES) instanceof List) {
                List<Number> coordinates = (List<Number>) data.get(FIELD_COORDINATES);
                if (coordinates.size() < 2) throw new RuntimeException("Expecting two coordinates when reading point");
                shape = org.janusgraph.core.attribute.Geoshape.point(coordinates.get(1).doubleValue(), coordinates.get(0).doubleValue());
            } else {
                try {
                    final String json = mapWriter.writeValueAsString(data.get("geometry"));
                    shape = new org.janusgraph.core.attribute.Geoshape(HELPER.getGeojsonReader().read(new StringReader(json)));
                } catch (IOException | ParseException e) {
                    throw new RuntimeException("I/O exception reading geoshape", e);
                }
            }
            return shape;
        }
    }

    /**
     * Geoshape binary serializer using spatial4j's {@link org.locationtech.spatial4j.io.BinaryCodec}.
     */
    public static class GeoshapeBinarySerializer {
        /**
         * Serialize a geoshape.
         */
        public static void write(OutputStream outputStream, org.janusgraph.core.attribute.Geoshape attribute) throws IOException {
            try (DataOutputStream dataOutput = new DataOutputStream(outputStream)) {
                HELPER.write(dataOutput, attribute);
                dataOutput.flush();
            }
            outputStream.flush();
        }

        /**
         * Deserialize a geoshape.
         */
        public static org.janusgraph.core.attribute.Geoshape read(InputStream inputStream) throws IOException {
            try (DataInputStream dataInput = new DataInputStream(inputStream)) {
                return new org.janusgraph.core.attribute.Geoshape(HELPER.readShape(dataInput));
            }
        }
    }

}
