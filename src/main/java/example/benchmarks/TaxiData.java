package example.benchmarks;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

public class TaxiData {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final ZoneOffset LOCAL_ZONE_OFFSET = ZoneOffset.ofHours(0);

    @JsonProperty("total_amount")
    private double totalAmount;

    @JsonProperty("improvement_surcharge")
    private double improvementSurcharge;

    @JsonProperty("pickup_location")
    private double[] pickupLocation;

    @JsonProperty("dropoff_location")
    private double[] dropOffLocation;

    @JsonProperty("pickup_datetime")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime pickupDateTime;

    @JsonProperty("dropoff_datetime")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime dropOffDateTime;

    @JsonProperty("trip_type")
    private int tripType;

    @JsonProperty("rate_code_id")
    private int rateCodeId;

    @JsonProperty("tolls_amount")
    private double tollsAmount;

    @JsonProperty("passenger_count")
    private int passengerCount;

    @JsonProperty("fare_amount")
    private double fareAmount;

    @JsonProperty("extra")
    private double extra;

    @JsonProperty("trip_distance")
    private double tripDistance;

    @JsonProperty("tip_amount")
    private double tipAmount;

    @JsonProperty("store_and_fwd_flag")
    private String storeAndFwdFlag;

    @JsonProperty("payment_type")
    private String paymentType;

    @JsonProperty("mta_tax")
    private double mtaTax;

    @JsonProperty("vendor_id")
    private String vendorId;

    private String description;

    public TaxiData() {

    }

    public TaxiData(String jsonEntry) throws JsonProcessingException {
        TaxiData taxiData = OBJECT_MAPPER.readValue(jsonEntry, TaxiData.class);
        this.totalAmount = taxiData.getTotalAmount();
        this.improvementSurcharge = taxiData.getImprovementSurcharge();
        this.dropOffLocation = taxiData.getDropOffLocation();
        this.pickupLocation = taxiData.getPickupLocation();
        this.pickupDateTime = taxiData.getPickupDateTime();
        this.dropOffDateTime = taxiData.getDropOffDateTime();
        this.tripType = taxiData.getTripType();
        this.tripDistance = taxiData.getTripDistance();
        this.tipAmount = taxiData.getTipAmount();
        this.rateCodeId = taxiData.getRateCodeId();
        this.tollsAmount = taxiData.getTollsAmount();
        this.passengerCount = taxiData.getPassengerCount();
        this.fareAmount = taxiData.getFareAmount();
        this.extra = taxiData.getExtra();
        this.storeAndFwdFlag = taxiData.getStoreAndFwdFlag();
        this.paymentType = taxiData.getPaymentType();
        this.mtaTax = taxiData.getMtaTax();
        this.vendorId = taxiData.getVendorId();
        this.description = String.format("Picked up %s passengers at %s and dropped at %s and" +
                        " total amount charged was %s with a distance of %s",
                passengerCount,
                pickupDateTime,
                dropOffDateTime,
                totalAmount,
                tripDistance);
    }

    public Document toDocument() {
        Document document = new Document();

        document.add(new DoublePoint("totalAmount", totalAmount));
        document.add(new StoredField("totalAmount", totalAmount));

        document.add(new DoublePoint("improvementSurcharge", improvementSurcharge));
        document.add(new StoredField("improvementSurcharge", improvementSurcharge));

        document.add(new DoublePoint("pickUpLocation", pickupLocation[0], pickupLocation[1]));

        document.add(new LongPoint("pickUpDateTime", pickupDateTime.toEpochSecond(LOCAL_ZONE_OFFSET)));
        document.add(new StoredField("pickUpDateTime", pickupDateTime.toEpochSecond(LOCAL_ZONE_OFFSET)));

        document.add(new DoublePoint("dropOffLocation", dropOffLocation[0], dropOffLocation[1]));

        document.add(new LongPoint("dropOffDateTime", dropOffDateTime.toEpochSecond(LOCAL_ZONE_OFFSET)));
        document.add(new StoredField("dropOffDateTime", dropOffDateTime.toEpochSecond(LOCAL_ZONE_OFFSET)));

        document.add(new IntPoint("tripType", tripType));
        document.add(new StoredField("tripType", tripType));

        document.add(new DoublePoint("tripDistance", tripDistance));
        document.add(new StoredField("tripDistance", tripDistance));

        document.add(new DoublePoint("tipAmount", tipAmount));
        document.add(new StoredField("tipAmount", tipAmount));

        document.add(new IntPoint("rateCodeId", rateCodeId));
        document.add(new StoredField("rateCodeId", rateCodeId));

        document.add(new DoublePoint("tollsAmount", tollsAmount));
        document.add(new StoredField("tollsAmount", tollsAmount));

        document.add(new IntPoint("passengerCount", passengerCount));
        document.add(new StoredField("passengerCount", passengerCount));

        document.add(new DoublePoint("fareAmount", fareAmount));
        document.add(new StoredField("fareAmount", fareAmount));

        document.add(new DoublePoint("extra", extra));
        document.add(new StoredField("extra", extra));

        document.add(new DoublePoint("tripDistance", tripDistance));
        document.add(new StoredField("tripDistance", tripDistance));

        document.add(new DoublePoint("tipAmount", tipAmount));
        document.add(new StoredField("tipAmount", tipAmount));

        document.add(new DoublePoint("mtaTax", mtaTax));
        document.add(new StoredField("mtaTax", mtaTax));

        document.add(new TextField("storeAndFwdFlag", storeAndFwdFlag, Field.Store.YES));
        document.add(new TextField("paymentType", paymentType, Field.Store.YES));
        document.add(new TextField("vendorId", vendorId, Field.Store.YES));
        document.add(new TextField("description", description, Field.Store.YES));

        return document;
    }

    public Object getFieldFromName(String fieldName) {

        String lowerCaseFieldName = fieldName.toLowerCase();

        return switch (lowerCaseFieldName) {
            case "total_amount" -> totalAmount;
            case "improvement_surcharge" -> improvementSurcharge;
            case "pickup_datetime" -> pickupDateTime;
            case "dropoff_datetime" -> dropOffDateTime;
            case "trip_distance" -> tripDistance;
            case "trip_type" -> tripType;
            case "rate_code_id" -> rateCodeId;
            case "tolls_amount" -> tollsAmount;
            case "passenger_count" -> passengerCount;
            case "mta_tax" -> mtaTax;
            case "vendor_id" -> vendorId;
            case "payment_type" -> paymentType;
            case "store_and_fwd_flag" -> storeAndFwdFlag;
            case "tip_amount" -> tipAmount;
            case "extra" -> extra;
            case "fare_amount" -> fareAmount;
            default -> throw new IllegalArgumentException("Unknown field " + fieldName);
        };

    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public double getImprovementSurcharge() {
        return improvementSurcharge;
    }

    public double[] getPickupLocation() {
        return pickupLocation;
    }

    public double[] getDropOffLocation() {
        return dropOffLocation;
    }

    public LocalDateTime getPickupDateTime() {
        return pickupDateTime;
    }

    public LocalDateTime getDropOffDateTime() {
        return dropOffDateTime;
    }

    public int getTripType() {
        return tripType;
    }

    public int getRateCodeId() {
        return rateCodeId;
    }

    public double getTollsAmount() {
        return tollsAmount;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public double getFareAmount() {
        return fareAmount;
    }

    public double getExtra() {
        return extra;
    }

    public double getTripDistance() {
        return tripDistance;
    }

    public double getTipAmount() {
        return tipAmount;
    }

    public String getStoreAndFwdFlag() {
        return storeAndFwdFlag;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public double getMtaTax() {
        return mtaTax;
    }

    public String getVendorId() {
        return vendorId;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "TaxiData{" +
                "totalAmount=" + totalAmount +
                ", improvementSurcharge=" + improvementSurcharge +
                ", pickupLocation=" + Arrays.toString(pickupLocation) +
                ", dropOffLocation=" + Arrays.toString(dropOffLocation) +
                ", pickupDateTime=" + pickupDateTime +
                ", dropOffDateTime=" + dropOffDateTime +
                ", tripType=" + tripType +
                ", rateCodeId=" + rateCodeId +
                ", tollsAmount=" + tollsAmount +
                ", passengerCount=" + passengerCount +
                ", fareAmount=" + fareAmount +
                ", extra=" + extra +
                ", tripDistance=" + tripDistance +
                ", tipAmount=" + tipAmount +
                ", storeAndFwdFlag='" + storeAndFwdFlag + '\'' +
                ", paymentType='" + paymentType + '\'' +
                ", mtaTax=" + mtaTax +
                ", vendorId='" + vendorId + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

}
