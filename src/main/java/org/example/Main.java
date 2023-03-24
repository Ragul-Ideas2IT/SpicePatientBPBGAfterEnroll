package org.example;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.opencsv.CSVWriter;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        MongoDatabase database = connectToDB();
//      get Patient details from patienttracker collection
//        2019-01-01 ----> 1546300800000L
//        2020-01-01 ----> 1577836800000L
//        2021-01-01 ----> 1609459200000L
//        2021-08-01 ----> 1627776000000L
//        2022-01-01 ----> 1640995200000L
        long endDate = 1627776000000L;
        long startDate = 1609459200000L;
        long end;
        MongoCursor<Document> patienttrackers = database.getCollection("patienttracker").aggregate(Arrays.asList(new Document("$match",
                        new Document("is_deleted", false)
//                                .append("_id", new ObjectId("5f56ebbfb8ad0741f2ebe9c8"))
                                .append("patient_status", "ENROLLED").append("enrollment_at",
                new Document("$lt",
                        new java.util.Date(endDate)).append("$gte", new Date(startDate)))),
                new Document("$sort", new Document("enrollment_at", 1)),
                new Document("$set",
                        new Document("enrollment_at",
                                new Document("$dateToString",
                                        new Document("format", "%Y-%m-%d")
                                                .append("date", "$enrollment_at"))))
//                new Document("$limit", 100L)
        )).cursor();
        long patientCount =
                database.getCollection("patienttracker").countDocuments(Filters.and(Filters.eq("is_deleted", false),
                        Filters.eq("patient_status", "ENROLLED"),
                        Filters.gte("enrollment_at", new java.util.Date(startDate)), Filters.lt(
                                "enrollment_at",
                                new java.util.Date(endDate))));
        List<String> fields = new ArrayList<>();
        double count = 0;
        fields.add("_id");
        fields.add("program_id");
        fields.add("national_id");
        fields.add("first_name");
        fields.add("last_name");
        fields.add("age");
        fields.add("gender");
        fields.add("confirm_diagnosis");
        fields.add("provisional_diagnosis");
//        CVD Risk Score
        fields.add("cvd_risk_score");
        fields.add("cvd_risk_level");
//        Health Insurance Status (Yes/No)
        fields.add("insurance_status");
//        Health Facility Level (Site characteristics Level)
        fields.add("site_id");
        fields.add("site");
        fields.add("site_level");
//        Country
        fields.add("country");
//                County
        fields.add("county");
//        Sub-county
        fields.add("sub_county");
        fields.add("height");
        fields.add("weight");
//        BMI
        fields.add("bmi");
//        Tobacco Use Status
        fields.add("tobacco_status");
//        Alcohol Use Status
        fields.add("alcohol_status");
//        Enrollment Type (Screening vs. Direct)
        fields.add("enrollment_type");
//        Enrollment Date
        fields.add("enrollment_at");
        fields.add("enrollment_bp_avg_systolic");
        fields.add("enrollment_bp_avg_diastolic");
        fields.add("enrollment_glucose_value");
        fields.add("enrollment_glucose_type");
        fields.add("bp_taken_on");
        fields.add("bp_avg_diastolic");
        fields.add("bp_avg_systolic");
        fields.add("bp_assessment_type");
        fields.add("bp_cvd_risk_level");
        fields.add("bp_cvd_risk_score");
        fields.add("bg_taken_on");
        fields.add("bg_glucose_value");
        fields.add("bg_glucose_type");
        fields.add("bg_assessment_type");
        List<String[]> values = new ArrayList<>();
        while (patienttrackers.hasNext()) {
            Document patienttracker = patienttrackers.next();
            String patientTrackId = patienttracker.get("_id").toString();
            long run = System.currentTimeMillis();
            Date d = new Date(run-start);
            System.out.print("Running Time: " + (run - start) + " ms --- "+(d.getHours() >= 5 ? d.getHours()-5:
                    d.getHours()) + " hr "+(d.getMinutes() >= 30 ? d.getMinutes()-30: d.getMinutes())+
                    " min "+d.getSeconds()+ " sec --- " + (count++ * 100) / patientCount + " --- " + patientTrackId+
                    "\r");
            String patientId = patienttracker.get("patient_id").toString();
            String[] patientCSV = new String[fields.size()];
            for (String key : patienttracker.keySet()) {
                if (fields.contains(key)) {
                    if (key.equals("country") || key.equals("site")) {
                        Document document = database.getCollection(key).find(Filters.and(Filters.eq(
                                "_id", new ObjectId(patienttracker.get(key).toString())))).first();
                        patientCSV[fields.indexOf(key)] = document.get("name").toString();
                        if (key.equals("site")) {
                            patientCSV[fields.indexOf("site_id")] = document.get("_id").toString();
                            patientCSV[fields.indexOf("site_level")] = document.get("site_level").toString();
                        }
                    } else {
                        patientCSV[fields.indexOf(key)] = patienttracker.get(key).toString();
                    }
                }
            }
            String enrollmentDate = convertDateToStringIfDate(patienttracker.get(
                    "enrollment_at").toString());
            patientCSV[fields.indexOf("enrollment_at")] = enrollmentDate;
            Document patient = database.getCollection("patient").find(Filters.and(Filters.eq("is_deleted", false),
                    Filters.eq("_id", new ObjectId(patientId)))).first();
            Document patientTobacco = database.getCollection("patientlifestyle").find(Filters.and(Filters.eq(
                            "is_deleted",
                            false),
                    Filters.eq("patient_track_id", new ObjectId(patientTrackId)),
                    Filters.eq("lifestyle_id", new ObjectId("625d7e0c47e4c0ab79a2a4ad")))).first();
            Document patientAlcohol = database.getCollection("patientlifestyle").find(Filters.and(Filters.eq(
                            "is_deleted",
                            false),
                    Filters.eq("patient_track_id", new ObjectId(patientTrackId)),
                    Filters.eq("lifestyle_id", new ObjectId("622b1164b5b029f13b626a60")))).first();
//            Document enrollmentBP = database.getCollection("bplog").find(Filters.and(Filters.eq("is_deleted", false),
//                    Filters.eq("patient_track_id", patientTrackId), Filters.eq("bp_taken_on", )))
            List<Document> enrollmentBPQuery = Arrays.asList(new Document("$match",
                            new Document("is_deleted", false)
                                    .append("patient_track_id",
                                            new ObjectId(patientTrackId))),
                    new Document("$project",
                            new Document("avg_diastolic", 1L)
                                    .append("avg_systolic", 1L)
                                    .append("bp_taken_on", 1L)
                                    .append("patient_track_id", 1L)),
                    new Document("$lookup",
                            new Document("from", "patienttracker")
                                    .append("localField", "patient_track_id")
                                    .append("foreignField", "_id")
                                    .append("as", "patienttracker")),
                    new Document("$unwind",
                            new Document("path", "$patienttracker")),
                    new Document("$set",
                            new Document("datediff",
                                    new Document("$dateDiff",
                                            new Document("startDate", "$patienttracker.enrollment_at")
                                                    .append("endDate", "$bp_taken_on")
                                                    .append("unit", "day")))),
                    new Document("$project",
                            new Document("patienttracker", 0L)),
                    new Document("$match",
                            new Document("datediff", 0L)),
                    new Document("$sort",
                            new Document("bp_taken_on", -1L)));
            Document enrollmentBP = database.getCollection("bplog").aggregate(enrollmentBPQuery).first();
            List<Document> enrollmentBGQuery = Arrays.asList(new Document("$match",
                            new Document("is_deleted", false)
                                    .append("patient_track_id",
                                            new ObjectId(patientTrackId))),
                    new Document("$set",
                            new Document("glucose_value",
                                    new Document("$cond", Arrays.asList(new Document("$lt", Arrays.asList("$glucose_value", -100L)), "$hba1c", "$glucose_value")))
                                    .append("glucose_type",
                                            new Document("$cond", Arrays.asList(new Document("$lt", Arrays.asList("$glucose_value", -100L)), "hba1c", "$glucose_type")))),
                    new Document("$project",
                            new Document("glucose_value", 1L)
                                    .append("glucose_type", 1L)
                                    .append("bg_taken_on", 1L)
                                    .append("patient_track_id", 1L)),
                    new Document("$lookup",
                            new Document("from", "patienttracker")
                                    .append("localField", "patient_track_id")
                                    .append("foreignField", "_id")
                                    .append("as", "patienttracker")),
                    new Document("$unwind",
                            new Document("path", "$patienttracker")),
                    new Document("$set",
                            new Document("datediff",
                                    new Document("$dateDiff",
                                            new Document("startDate", "$patienttracker.enrollment_at")
                                                    .append("endDate", "$bg_taken_on")
                                                    .append("unit", "day")))),
                    new Document("$project",
                            new Document("patienttracker", 0L)),
                    new Document("$match",
                            new Document("datediff", 0L)),
                    new Document("$sort",
                            new Document("bg_taken_on", -1L)));
            Document enrollmentBG = database.getCollection("glucoselog").aggregate(enrollmentBGQuery).first();
            for (String field : fields) {
                switch (field) {
                    case "insurance_status":
                        patientCSV[fields.indexOf(field)] = patient.get("insurance_status").toString();
                        break;

                    case "county":
                        patientCSV[fields.indexOf(field)] =
                                database.getCollection(field).find(Filters.and(Filters.eq("_id", new ObjectId(patient.get(
                                        field).toString())))).first().get("name").toString();
                        break;

                    case "sub_county":
                        patientCSV[fields.indexOf(field)] =
                                database.getCollection("subcounty").find(Filters.and(Filters.eq("_id",
                                        new ObjectId(patient.get(
                                field).toString())))).first().get("name").toString();
                        break;

                    case "tobacco_status":
                        if (patientTobacco != null) {
                            patientCSV[fields.indexOf(field)] = patientTobacco.get("lifestyle_answer").toString();
                        } else {
                            patientCSV[fields.indexOf(field)] = "-";
                        }
                        break;

                    case "alcohol_status":
                        if (patientAlcohol != null) {
                            patientCSV[fields.indexOf(field)] = patientAlcohol.get("lifestyle_answer").toString();
                        } else {
                            patientCSV[fields.indexOf(field)] = "-";
                        }
                        break;

                    case "enrollment_type":
                        if (patienttracker.containsKey("screening_id")) {
                            patientCSV[fields.indexOf(field)] = "From Screening";
                        } else {
                            patientCSV[fields.indexOf(field)] = "Direct Enrollment";
                        }
                        break;

                    case "enrollment_bp_avg_systolic":
                        if (enrollmentBP != null) {
                            patientCSV[fields.indexOf(field)] = enrollmentBP.get("avg_systolic").toString();
                        } else {
                            patientCSV[fields.indexOf(field)] = "-";
                        }
                        break;

                    case "enrollment_bp_avg_diastolic":
                        if (enrollmentBP != null) {
                            patientCSV[fields.indexOf(field)] = enrollmentBP.get("avg_diastolic").toString();
                        } else {
                            patientCSV[fields.indexOf(field)] = "-";
                        }
                        break;

                    case "enrollment_glucose_value":
                        if (enrollmentBG != null) {
                            patientCSV[fields.indexOf(field)] = enrollmentBG.get("glucose_value").toString();
                        } else {
                            patientCSV[fields.indexOf(field)] = "-";
                        }
                        break;

                    case "enrollment_glucose_type":
                        if (enrollmentBG != null) {
                            patientCSV[fields.indexOf(field)] = enrollmentBG.get("glucose_type").toString();
                        } else {
                            patientCSV[fields.indexOf(field)] = "-";
                        }
                        break;
                }
            }
            List<Document> assBPQuery = Arrays.asList(new Document("$match",
                            new Document("is_deleted", false)
                                    .append("patient_track_id",
                                            new ObjectId(patientTrackId)).append("type",
                                            new Document("$ne", "screening"))),
                    new Document("$project",
                            new Document("avg_diastolic", 1L)
                                    .append("avg_systolic", 1L)
                                    .append("bp_taken_on", 1L)
                                    .append("patient_track_id", 1L)
                                    .append("type", 1L).append("cvd_risk_level", 1L).append("cvd_risk_score", 1L)),
                    new Document("$lookup",
                            new Document("from", "patienttracker")
                                    .append("localField", "patient_track_id")
                                    .append("foreignField", "_id")
                                    .append("as", "patienttracker")
                                    .append("pipeline", Arrays.asList(new Document("$match",
                                            new Document("is_deleted", false)
                                                    .append("patient_status", "ENROLLED"))))),
                    new Document("$unwind",
                            new Document("path", "$patienttracker")),
                    new Document("$set",
                            new Document("datediff",
                                    new Document("$dateDiff",
                                            new Document("startDate", "$patienttracker.enrollment_at")
                                                    .append("endDate", "$bp_taken_on")
                                                    .append("unit", "day")))),
                    new Document("$set",
                            new Document("timediff",
                                    new Document("$dateDiff",
                                            new Document("startDate", "$patienttracker.enrollment_at")
                                                    .append("endDate", "$bp_taken_on")
                                                    .append("unit", "second")))),

                    new Document("$set",
                            new Document("bp_taken_on",
                                    new Document("$dateToString",
                                            new Document("format", "%Y-%m-%d")
                                                    .append("date", "$bp_taken_on")))),
                    new Document("$project",
                            new Document("patienttracker", 0L)),
                    new Document("$match",
                            new Document("datediff",
                                    new Document("$gt", 0L))),
                    new Document("$sort",
                            new Document("timediff", -1L)));
            List<Document> assBGQuery = Arrays.asList(new Document("$match",
                            new Document("is_deleted", false).append("patient_track_id",
                                    new ObjectId(patientTrackId)).append("type",
                                    new Document("$ne", "screening"))),
                    new Document("$set",
                            new Document("glucose_value",
                                    new Document("$cond", Arrays.asList(new Document("$lt", Arrays.asList("$glucose_value", -100L)), "$hba1c", "$glucose_value")))
                                    .append("glucose_type",
                                            new Document("$cond", Arrays.asList(new Document("$lt", Arrays.asList("$glucose_value", -100L)), "hba1c", "$glucose_type")))),
                    new Document("$project",
                            new Document("glucose_value", 1L)
                                    .append("glucose_type", 1L)
                                    .append("bg_taken_on", 1L)
                                    .append("patient_track_id", 1L)
                                    .append("type", 1L)),
                    new Document("$lookup",
                            new Document("from", "patienttracker")
                                    .append("localField", "patient_track_id")
                                    .append("foreignField", "_id")
                                    .append("as", "patienttracker")),
                    new Document("$unwind",
                            new Document("path", "$patienttracker")),
                    new Document("$addFields",
                            new Document("datediff",
                                    new Document("$dateDiff",
                                            new Document("startDate", "$patienttracker.enrollment_at")
                                                    .append("endDate", "$bg_taken_on")
                                                    .append("unit", "day")))),
                    new Document("$set",
                            new Document("timediff",
                                    new Document("$dateDiff",
                                            new Document("startDate", "$patienttracker.enrollment_at")
                                                    .append("endDate", "$bg_taken_on")
                                                    .append("unit", "second")))),
                    new Document("$match",
                            new Document("datediff",
                                    new Document("$gt", 0L))),
                    new Document("$project",
                            new Document("patienttracker", 0L)),
                    new Document("$sort",
                            new Document("timediff", -1L)),
                    new Document("$set",
                            new Document("bg_taken_on",
                                    new Document("$dateToString",
                                            new Document("format", "%Y-%m-%d")
                                                    .append("date", "$bg_taken_on")))));
            Iterator<Document> bpIterator = database.getCollection("bplog").aggregate(assBPQuery).iterator();
            Iterator<Document> bgIterator = database.getCollection("glucoselog").aggregate(assBGQuery).iterator();
            List<String> bpTakenOn = new ArrayList<>();
            List<String> avgSystolic = new ArrayList<>();
            List<String> avgDiastolic = new ArrayList<>();
            List<String> bpType = new ArrayList<>();
            List<String> bpCvdRiskLevel = new ArrayList<>();
            List<String> bpCvdRiskScore = new ArrayList<>();
            List<String> bgTakenOn = new ArrayList<>();
            List<String> glucoseValue = new ArrayList<>();
            List<String> glucoseType = new ArrayList<>();
            List<String> bgType = new ArrayList<>();
            String bpdatediff = "";
            String bgdatediff = "";
            while (bpIterator.hasNext()) {
                Document bplog = bpIterator.next();
                if ((bplog.get("datediff") != null)) {
                    if (!bplog.get("datediff").toString().equals(bpdatediff)) {
//                        if(!bplog.get("type").toString().equals(
//                                "assessment") && !(Integer.parseInt(bplog.get("datediff").toString()) < 0)) {
                        bpTakenOn.add(bplog.get("bp_taken_on").toString());
                        avgSystolic.add(bplog.get("avg_systolic").toString());
                        avgDiastolic.add(bplog.get("avg_diastolic").toString());
                        bpType.add(bplog.get("type").toString());
                        if (bplog.get("cvd_risk_level") != null) {
                            bpCvdRiskLevel.add(bplog.get("cvd_risk_level").toString());
                        } else {
                            bpCvdRiskLevel.add("-");
                        }
                        if (bplog.get("cvd_risk_score") != null) {
                            bpCvdRiskScore.add(bplog.get("cvd_risk_score").toString());
                        } else {
                            bpCvdRiskScore.add("-");
                        }
                        bpdatediff = bplog.get("datediff").toString();
//                    }
                    }
                }
            }
            while (bgIterator.hasNext()) {
                Document glucoselog = bgIterator.next();
                if ((glucoselog.get("datediff") != null)) {
                    if (!glucoselog.get("datediff").toString().equals(bgdatediff)) {
//                        if(!bplog.get("type").toString().equals(
//                                "assessment") && !(Integer.parseInt(bplog.get("datediff").toString()) < 0)) {
                        bgTakenOn.add(glucoselog.get("bg_taken_on").toString());
                        glucoseValue.add(glucoselog.get("glucose_value").toString());
                        glucoseType.add(glucoselog.get("glucose_type").toString());
                        bgType.add(glucoselog.get("type").toString());
                        bgdatediff = glucoselog.get("datediff").toString();
//                    }
                    }
                }
            }
//            if (bpdatediff.equals("")) {
            if (bpTakenOn.isEmpty()) {
                patientCSV[fields.indexOf("bp_taken_on")] = "-";
                patientCSV[fields.indexOf("bp_avg_systolic")] = "-";
                patientCSV[fields.indexOf("bp_avg_diastolic")] = "-";
                patientCSV[fields.indexOf("bp_assessment_type")] = "-";
                patientCSV[fields.indexOf("bp_cvd_risk_level")] = "-";
                patientCSV[fields.indexOf("bp_cvd_risk_score")] = "-";
            } else {
                patientCSV[fields.indexOf("bp_taken_on")] = bpTakenOn.get(0);
                patientCSV[fields.indexOf("bp_avg_systolic")] = avgSystolic.get(0);
                patientCSV[fields.indexOf("bp_avg_diastolic")] = avgDiastolic.get(0);
                patientCSV[fields.indexOf("bp_assessment_type")] = bpType.get(0);
                patientCSV[fields.indexOf("bp_cvd_risk_level")] = bpCvdRiskLevel.get(0);
                patientCSV[fields.indexOf("bp_cvd_risk_score")] = bpCvdRiskScore.get(0);
            }
            if (bgTakenOn.isEmpty()) {
                patientCSV[fields.indexOf("bg_taken_on")] = "-";
                patientCSV[fields.indexOf("bg_glucose_value")] = "-";
                patientCSV[fields.indexOf("bg_glucose_type")] = "-";
                patientCSV[fields.indexOf("bg_assessment_type")] = "-";
            } else {
                patientCSV[fields.indexOf("bg_taken_on")] = bgTakenOn.get(0);
                patientCSV[fields.indexOf("bg_glucose_value")] = glucoseValue.get(0);
                patientCSV[fields.indexOf("bg_glucose_type")] = glucoseType.get(0);
                patientCSV[fields.indexOf("bg_assessment_type")] = bgType.get(0);
            }
            values.add(patientCSV);
//            long run = System.currentTimeMillis();
//            Date d = new Date(run-start);
//            System.out.print("Running Time: " + (run - start) + " ms --- "+(d.getHours() >= 5 ? d.getHours()-5:
//                    d.getHours()) + " hr "+(d.getMinutes() >= 30 ? d.getMinutes()-30: d.getMinutes())+
//                            " min "+d.getSeconds()+ " sec --- " + (count++ * 100) / patientCount + "%\r");
//                System.out.println(patientTrackId);
//            }
//            if (!list.isEmpty()) {
//                for (int i = 1; i < arrayMax; i++) {
//                    String[] temp = new String[fields.size()];
//                    for (Map.Entry<String, String[]> entry : list.entrySet()) {
//                        if ((entry.getValue().length > i) && (!Objects.equals(entry.getValue()[i], ""))) {
//                            temp[fields.indexOf(entry.getKey())] = entry.getValue()[i].trim();
//                        }
//                    }
//                    values.add(temp);
//                }
//            }
            for (int i = 1; i < Math.max(bpTakenOn.size(), bgTakenOn.size()); i++) {
//                String[] patientCSVDup = patientCSV;
                String[] patientCSVDup = new String[100];
                patientCSVDup[fields.indexOf("bp_taken_on")] = (i < bpTakenOn.size()) ? bpTakenOn.get(i) : "-";
                patientCSVDup[fields.indexOf("bp_avg_systolic")] = (i < bpTakenOn.size()) ? avgSystolic.get(i) : "-";
                patientCSVDup[fields.indexOf("bp_assessment_type")] = (i < bpTakenOn.size()) ? bpType.get(i) : "-";
                patientCSVDup[fields.indexOf("bp_avg_diastolic")] = (i < bpTakenOn.size()) ? avgDiastolic.get(i) : "-";
                patientCSVDup[fields.indexOf("bp_cvd_risk_level")] = (i < bpTakenOn.size()) ? bpCvdRiskLevel.get(i) : "-";
                patientCSVDup[fields.indexOf("bp_cvd_risk_score")] = (i < bpTakenOn.size()) ? bpCvdRiskScore.get(i) : "-";
                patientCSVDup[fields.indexOf("bg_taken_on")] = (i < bgTakenOn.size()) ? bgTakenOn.get(i) : "-";
                patientCSVDup[fields.indexOf("bg_glucose_value")] = (i < bgTakenOn.size()) ? glucoseValue.get(i) : "-";
                patientCSVDup[fields.indexOf("bg_glucose_type")] = (i < bgTakenOn.size()) ? glucoseType.get(i) : "-";
                patientCSVDup[fields.indexOf("bg_assessment_type")] = (i < bgTakenOn.size()) ? bgType.get(i) : "-";
//                System.out.println(patientTrackId);
                values.add(patientCSVDup);
            }
        }
        System.out.println("Exported!");
        csvWriter(fields, values);
        end = System.currentTimeMillis();
        System.out.println("Timetaken: " + (end - start) + " ms");
    }

    public static String convertDateToStringIfDate(String date) {
        String convertedString = date;
        if (Pattern.matches("[0-9]{4}(-)[0-9]{2}(-)[0-9]{2}(T)[0-9]{2}(:)[0-9]{2}(:)[0-9]{2}", date)) {
            convertedString = date.substring(0, 9);
        }
        return convertedString;
    }

    public static MongoDatabase connectToDB() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017/"));
        MongoDatabase database = mongoClient.getDatabase("spice_dump_17_03_2023");
        System.out.println("Successfully Connected to the database");
        return database;
    }

    public static void csvWriter(List<String> fields, List<String[]> values) throws IOException {
        CSVWriter csvWriter = new CSVWriter(new FileWriter("Jan2021-Jul2021.csv"));
        csvWriter.writeNext(fields.toArray(new String[0]));
        csvWriter.writeAll(values);
        csvWriter.close();
    }
}