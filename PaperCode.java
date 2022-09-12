package com.parper;

import java.util.*;

/**
 * Created by jiangxin on 22/9/12.
 */
public class PaperCode {

    public static java.util.Map<String, String> detailDataMap = new java.util.HashMap<String, String>();
//    public static java.util.Set<String> iso3DataSet = new java.util.HashSet<String>();
    public static java.util.Set<String> companyProductSet = new java.util.LinkedHashSet<String>();

    public static java.util.Map<String, com.parper.PaperCode.MaxMinYear> companyProductYearMap =
            new java.util.HashMap<String, com.parper.PaperCode.MaxMinYear>();

    public static java.util.Map<String, java.util.List<String>> companyProductYearToISOMap =
            new java.util.HashMap<String, java.util.List<String>>();

    public static String detailDataPath = "/Users/jiangxin/Downloads/hejinqiu/data_0914.csv";
//    public static String iso3DataPath = "/User/didi/Download/hejinqiu/detail_data.csv";
    public static String distanceOutputPath = "/Users/jiangxin/Downloads/hejinqiu/distance.csv";
    public static String neighbourOutputPath = "/Users/jiangxin/Downloads/hejinqiu/neighbour.csv";

    public static String administrativeDistancePath = "";
    public static String geographicDistancePath = "";


    public static Map<String, Integer> administrativeTitleMap = new java.util.HashMap<String, Integer>();
    public static List<List<Double>> administrativeDistanceList = new java.util.ArrayList<java.util.List<Double>>();

    public static Map<String, Integer> geographicTitleMap = new java.util.HashMap<>();
    public static List<List<Double>> geographicDistanceList = new java.util.ArrayList<>();

    public static Map<String, Set<String>> hs96ISO3YearKeyOfCityMap = new java.util.HashMap<String, java.util.Set<String>>();
    public static  Map<String, Set<String>> hs96ISO3YearKeyOfProvinceMap = new java.util.HashMap<String, java.util.Set<String>>();

    public static String title = "coid,hs96,ISO3,year,vcexp,indcode,last_year_vcexp" +
            ",adm_nearest_iso,adm_nearest_iso_vcexp,adm_nearest_iso_distance" +
            ",geo_nearest_iso,geo_nearest_iso_vcexp,geo_nearest_iso_distance";

    public static String titleForNeighbour = "coid,hs96,ISO3,year,vcexp,indcode,last_year_vcexp" +
            "city_neighbour_company_num,province_neighbour_company_num"+
            ",city_neighbour_company_vcexp_average,city_neighbour_company_vcexp_max,city_neighbour_company_vcexp_min" +
            ",province_neighbour_company_vcexp_average,province_neighbour_company_vcexp_max,province_neighbour_company_vcexp_min";



    public static void main (String args[]) throws java.io.IOException {

        long startTime = System.currentTimeMillis();


        java.io.BufferedWriter outDistance =
                new java.io.BufferedWriter(new java.io.OutputStreamWriter(new java.io.FileOutputStream(distanceOutputPath), "GBK"));

        java.io.BufferedWriter outNeighbour =
                new java.io.BufferedWriter(new java.io.OutputStreamWriter(new java.io.FileOutputStream(neighbourOutputPath), "GBK"));


        readCsvFile(detailDataPath);
//        readCsvFile(iso3DataPath, iso3DataSet);

        readDistanceFile(administrativeDistancePath, administrativeTitleMap, administrativeDistanceList);
        readDistanceFile(geographicDistancePath, geographicTitleMap, geographicDistanceList);

        List<String> companyProductList = new java.util.ArrayList<String>(companyProductSet)
                .stream()
                .sorted()
                .collect(java.util.stream.Collectors.toList());

//        List<String> iso3DataList = new java.util.ArrayList<String>(iso3DataSet)
//                .stream()
//                .sorted()
//                .collect(java.util.stream.Collectors.toList());

        int companyProductCount = 0;
        outDistance.newLine();
        outNeighbour.write(titleForNeighbour);
        outNeighbour.newLine();

        for (String companyProduct : companyProductList) {
            companyProductCount++;
            if (companyProductCount % 5000 == 0) {
                System.out.println();
            }

            MaxMinYear maxMinYear = companyProductYearMap.get(companyProduct);
            for (int year = maxMinYear.getMaxYear(); year <= maxMinYear.getMaxYear(); year++) {
                String iso =  maxMinYear.getMaxYearISO();
//                for (String iso : iso3DataList) {
//                    String key = companyProduct + "_" + iso + "_" + year;

                    String resString = "";

//                    if (detailDataMap.containsKey(key)) {
//                        resString = detailDataMap.get(key);
//
//                    } else {
                        String maxYearKey = companyProduct + "_" + maxMinYear.getMaxYearISO() + "_" + maxMinYear.getMaxYear();
                        String detailInfo = detailDataMap.get(maxYearKey);
                        String[] detailArray = detailInfo.split(",", -1);

                        detailArray[3] = maxMinYear.getMaxYearISO();
                        detailArray[4] = String.valueOf(year);
                        detailArray[8] = String.valueOf(0);
                        resString = mkString(detailArray);
//                    }

                    String[] resStringArray = resString.split(",", -1);
                    String indcode = resStringArray[9];

                    if (!indcode.equals("")) {
                        int indcodeV = Integer.parseInt(indcode);
                        if (indcodeV >= 3800 && indcodeV <= 3999) {
                            continue;
                        }
                    }

                    String lastYearKey = companyProduct + "_" + iso + "_" + (year - 1);
                    String lastYearValue = detailDataMap.get(lastYearKey);
                    String lastYearVcexp = "";

                    if (lastYearValue != null) {
                        lastYearVcexp = lastYearValue.split(",", -1)[8];
                    }

                    String baseKey = resStringArray[1] + "," + resStringArray[2]
                            + "," + resStringArray[3] + "," + resStringArray[4]
                            + "," + resStringArray[8]  + "," + resStringArray[9] + "," + lastYearVcexp;

                    String nearestDistanceStr = getNearestDistance(companyProduct, (year - 1), iso);
                    String []nearestDistanceArray = nearestDistanceStr.split(",", -1);

                    String adminNearestCountryVcexp = nearestDistanceArray[1];
                    String geoNearestCountryVcexp = nearestDistanceArray[4];

                    if (!adminNearestCountryVcexp.equals("") && !geoNearestCountryVcexp.equals("")) {
                        String distanceString = baseKey + "," + nearestDistanceStr;
                        outDistance.write(distanceString);
                        outDistance.newLine();
                    }

                    String company = companyProduct.split("_")[0];
                    String product = companyProduct.split("_")[1];

                    String city = resStringArray[14];
                    String province = resStringArray[13];

                    int cityCompanySetSize = 0;
                    int provinceCompanySetSize = 0;

                    Set<String> cityComanySet = new java.util.HashSet<>();
                    Set<String> provinceCompanySet = new java.util.HashSet<>();

                    if (!city.equals("")) {
                        String cityKey = product + "_" + iso + "_" + (year - 1) + "_" + city;
                        cityComanySet = hs96ISO3YearKeyOfCityMap.getOrDefault(cityKey, new java.util.HashSet<>());
                        cityComanySet.remove(company);

                        cityCompanySetSize = cityComanySet.size();
                    }

                    if (!province.equals("")) {
                        String provinceKey = product + "_" + iso + "_" + (year - 1) + "_" + province;
                        provinceCompanySet = hs96ISO3YearKeyOfProvinceMap.getOrDefault(provinceKey, new java.util.HashSet<>());

                        provinceCompanySet.remove(company);
                        provinceCompanySetSize = provinceCompanySet.size();
                    }

                    String neighourStr = baseKey + "," + cityCompanySetSize + "," + provinceCompanySetSize;
                    String cityNeighbourIndicator = getIndicator(cityComanySet, product, iso, (year - 1));
                    String provinceNeighbourIndicator = getIndicator(provinceCompanySet, product, iso, (year - 1));

                    if (!cityNeighbourIndicator.equals("") && !provinceNeighbourIndicator.equals("")) {
                        neighourStr = neighourStr + "," + cityNeighbourIndicator + "," + provinceNeighbourIndicator;
                        outNeighbour.write(neighourStr);
                        outNeighbour.newLine();
                    }

                    if (System.currentTimeMillis() % 30000 == 0) {
                        System.out.println(resString);
                    }

//                }
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println("cost time: " + (endTime - startTime) % (1000 * 60) + "min");

        System.out.println("end");

        outDistance.flush();
        outDistance.close();

        outNeighbour.flush();
        outNeighbour.close();
    }

    private static String getIndicator(Set<String> companySet, String product, String iso, int year) {
        String average = "";
        String max = "";
        String min = "";

        double sumValue = 0.0;
        double maxValue = -1;
        double minValue = -1;
        int count = 0;

        java.util.Iterator<String> it = companySet.iterator();
        while (it.hasNext()) {
            String company = it.next();
            String key = company + "_" + product + "_" + iso + "_" + year;

            String value = detailDataMap.get(key);

            String vcexp = value.split(",", -1)[8];

            if (isNumeric(vcexp)) {
                long dataLong = Long.parseLong(vcexp);

                count++;
                sumValue += dataLong;

                if (maxValue == -1 || maxValue < dataLong) {
                    maxValue = dataLong;
                }

                if (minValue == -1 || minValue > dataLong) {
                    minValue = dataLong;
                }
            }
        }

        if (count > 0 ) {
            double averageV = sumValue / count;
            java.math.BigDecimal b = new java.math.BigDecimal(averageV);
            double precisionDoubleV = b.setScale(2, java.math.BigDecimal.ROUND_HALF_UP).doubleValue();
            average = String.valueOf(precisionDoubleV);

            max = String.valueOf(maxValue);
            min = String.valueOf(minValue);
        }

        String res = average + "," + max + "," + min;
        return res;
    }


    public final static boolean isNumeric(String s) {
        if (s != null && !"".equals(s.trim())) {
            return s.matches("^[0-9]*$");
        } else {
            return false;
        }
    }

    private static String getNearestDistance(String companyProduct, int year, String nowDest) {

        String adminNearestCountry = "";
        String adminNearestCountryVcexp = "";
        String adminNearestCountryDistance = "";
        String geoNearestCountry = "";
        String geoNearestCountryVcexp = "";
        String geoNearestCountryDistance = "";


        String key = companyProduct + "_" + year;
        List<String> destList = companyProductYearToISOMap.get(key);

        Double minAdminDistance = -1D;
        Double minGeoDistance = -1D;

        if (destList == null || destList.size() == 0) {

        } else {
            for (String lastDest : destList) {
                Integer lastDestCode = administrativeTitleMap.get(lastDest);
                Integer nowDestCode = administrativeTitleMap.get(nowDest);
                if (lastDestCode != null && nowDestCode != null && !nowDest.equals(lastDest)) {
                    Double adminDistance = (lastDestCode > nowDestCode) ?
                            administrativeDistanceList.get(lastDestCode).get(nowDestCode) :
                            administrativeDistanceList.get(nowDestCode).get(lastDestCode);

                    if (minAdminDistance == -1 || adminDistance < minAdminDistance) {
                        adminNearestCountry = lastDest;
                        minAdminDistance = adminDistance;
                    }

                }
                lastDestCode = geographicTitleMap.get(lastDest);
                nowDestCode = geographicTitleMap.get(nowDest);

                if (lastDestCode != null && nowDestCode != null && !nowDest.equals(lastDest)) {
                    Double geoDistance = (lastDestCode > nowDestCode) ?
                            geographicDistanceList.get(lastDestCode).get(nowDestCode) :
                            geographicDistanceList.get(nowDestCode).get(lastDestCode);

                    if (minGeoDistance == -1 || geoDistance < minGeoDistance) {
                        geoNearestCountry = lastDest;
                        minGeoDistance = geoDistance;
                    }
                }
            }
        }


        if (!adminNearestCountry.equals("")) {
            String tempKey = companyProduct + "_" + adminNearestCountry + "_" + year;
            String []tempArray = detailDataMap.get(tempKey).split(",", -1);
            adminNearestCountryVcexp = tempArray[8];
        }

        if (!geoNearestCountry.equals("")) {
            String tempKey = companyProduct + "_" + geoNearestCountry + "_" + year;
            String []tempArray = detailDataMap.get(tempKey).split(",", -1);
            geoNearestCountryVcexp = tempArray[8];
        }

        if (minAdminDistance != -1) {
            adminNearestCountryDistance = String.valueOf(minAdminDistance);
        }

        if (minGeoDistance != -1) {
            geoNearestCountryDistance = String.valueOf(minGeoDistance);
        }

        String result = adminNearestCountry + "," + adminNearestCountryVcexp + "," + adminNearestCountryDistance +
                "," + geoNearestCountry + "," + geoNearestCountryVcexp + "," + geoNearestCountryDistance;

        return result;
    }

    private static void readDistanceFile(String distancePath, Map<String, Integer> distanceTitleMap, List<List<Double>> distanceList) {
        int lineCount = 0;

        try {
            java.io.BufferedReader reader =
                    new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(distancePath), "utf8"));

            String line = null;
            while ((line = reader.readLine()) != null) {
                if (lineCount == 0) {
                    String[] array = line.split(",", -1);
                    int len = array.length;

                    for (int i=1; i<len; i++) {
                        distanceTitleMap.put(array[i], i -1);
                    }

                    lineCount++;
                } else {
                    String[] array = line.split(",", -1);
                    int len = lineCount;
                    List<Double> tempList = new java.util.ArrayList<>();

                    for (int i= 1;i < len + 1; i++) {
                        Double data = (array[i] == null || "".equals(array[i])) ? 0.0 :
                                Double.parseDouble(array[i]);

                        tempList.add(data);
                    }

                    distanceList.add(tempList);

                    lineCount++;
                }

            }

        } catch (java.io.FileNotFoundException e) {
            e.printStackTrace();
        } catch (java.io.UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeCSV(List<String> resultList, String outputPath) {
        try {
            java.io.BufferedWriter out = new java.io.BufferedWriter(new java.io.OutputStreamWriter(new java.io.FileOutputStream(outputPath), "UTF-8"));

            for (int i = 0;i < resultList.size(); i++ ) {
                out.write(resultList.get(i));
                out.newLine();
            }

            out.flush();
            out.close();
        } catch (java.io.FileNotFoundException e) {
            e.printStackTrace();
        } catch (java.io.UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    public static String mkString(String [] strArray) {
        StringBuffer sb = new StringBuffer();
        for (String str : strArray) {
            sb.append(str).append(",");
        }
        String resStr = sb.toString();
        return resStr.substring(0, resStr.length() - 1);
    }

    public static void readCsvFile(String path, java.util.Map<String, String> detailDataMap) {
        try {
            java.io.BufferedReader readder = new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(path), "GBK"));

            String line = null;
            while ((line = readder.readLine()) != null) {
                String item[] = line.split(",", -1);
                String key = item[1] + "_" + item[2] + "_" + item[3] + "_" + item[4];

                String companyProduct = item[1] + "_" + item[2];
                int year = Integer.parseInt(item[4]);
                String iso = item[3];

                detailDataMap.put(key, line);
                companyProductSet.add(companyProduct);

                if (!companyProductYearMap.containsKey(companyProduct)) {
                    MaxMinYear maxMinYear = new MaxMinYear(year, year, iso, iso);

                    companyProductYearMap.put(companyProduct, maxMinYear);

                } else {
                    MaxMinYear maxMinYear = companyProductYearMap.get(companyProduct);
                    int minYear = maxMinYear.minYear;
                    int maxYear = maxMinYear.maxYear;

                    minYear = Math.min(minYear, year);
                    maxYear = Math.max(maxYear, year);
                    maxMinYear.setMaxYear(maxYear);
                    maxMinYear.setMinYear(minYear);

                    companyProductYearMap.put(companyProduct, maxMinYear);
                }
            }

        } catch (java.io.FileNotFoundException e) {
            e.printStackTrace();
        } catch (java.io.UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }


    public static void readCsvFile(String path, java.util.Set<String> iso3DataSet) {
        try {
            java.io.BufferedReader reader =
                    new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(path), "utf8"));

            String line = null;
            while ((line = reader.readLine()) != null) {
                iso3DataSet.add(line);
            }
        } catch (java.io.FileNotFoundException e) {
            e.printStackTrace();
        } catch (java.io.UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    public static void readCsvFile(String path) {
        try {
            java.io.BufferedReader reader =
                    new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(path), "utf-8"));

//            cpynm	企业名称
//            coid	企业数字编号，纯数字
//            hs96	产品纯数字编码
//            ISO3	英文字母三位出口目的地代码
//            year	年份
//            vcexp	企业hs96产品当年在ISO3的出口额
//            Ind2	产业数字编码
//            trademode	贸易方式
//            province	企业所在的省（自治区、直辖市）
//            city	企业所在的地（区、市、州、盟）
//            county	企业所在的县（区、市、旗）

            System.out.println("title info: " + reader.readLine());
            String line = null;
            while ((line = reader.readLine()) != null) {
                String item[] = line.split(",", -1);

                String company = item[1]; // 1
                String product = item[6]; // 2
                String iso = item[8]; // 3
                Integer year = Integer.parseInt(item[0]); // 4

                String city = item[4];
                String province = item[3];

                String key = company + "_" + product + "_" + iso + "_" + year;
                System.out.println("line : " + line + ", key: " + key);

                String companyProduct = company + "_" + product;

                String companyProductYearKey = company + "_" + product + "_" + year;
                String hs96ISO3YearKey = product + "_" + iso + "_" + year;

                String hs96ISO3YearKeyOfCity = hs96ISO3YearKey + "_" + city;
                String hs96ISO3YearKeyOfProvince = hs96ISO3YearKey + "_" + province;

                detailDataMap.put(key, line);
                companyProductSet.add(companyProduct);

                if (!hs96ISO3YearKeyOfCityMap.containsKey(hs96ISO3YearKeyOfCity)) {
                    java.util.Set<String> tempSet = new java.util.HashSet<String>();
                    tempSet.add(company);

                    hs96ISO3YearKeyOfCityMap.put(hs96ISO3YearKeyOfCity, tempSet);
                } else {
                    java.util.Set<String> tempSet = hs96ISO3YearKeyOfCityMap.get(hs96ISO3YearKeyOfCity);
                    tempSet.add(company);
                    hs96ISO3YearKeyOfCityMap.put(hs96ISO3YearKeyOfCity, tempSet);
                }

                if (!hs96ISO3YearKeyOfProvinceMap.containsKey(hs96ISO3YearKeyOfProvince)) {
                    Set<String> tempSet = new java.util.HashSet<String>();
                    tempSet.add(company);
                    hs96ISO3YearKeyOfProvinceMap.put(hs96ISO3YearKeyOfProvince, tempSet);
                } else {
                    Set<String> tempSet = hs96ISO3YearKeyOfProvinceMap.get(hs96ISO3YearKeyOfProvince);
                    tempSet.add(company);

                    hs96ISO3YearKeyOfProvinceMap.put(hs96ISO3YearKeyOfProvince, tempSet);
                }

                if (!companyProductYearToISOMap.containsKey(companyProductYearKey)) {
                    List<String> tempList = new java.util.ArrayList<String>();
                    tempList.add(iso);
                    companyProductYearToISOMap.put(companyProductYearKey, tempList);
                } else {
                    List<String> tempList = new java.util.ArrayList<String>();
                    tempList.add(iso);
                    companyProductYearToISOMap.put(companyProductYearKey, tempList);
                }

                if(!companyProductYearMap.containsKey(companyProduct)) {
                    MaxMinYear maxMinYear = new com.parper.PaperCode.MaxMinYear(year, year, iso, iso);
                    companyProductYearMap.put(companyProduct, maxMinYear);
                } else {
                    MaxMinYear maxMinYear = companyProductYearMap.get(companyProduct);
                    int minYear = maxMinYear.getMinYear();
                    int maxYear = maxMinYear.getMaxYear();

                    if (year < minYear) {
                        maxMinYear.setMinYear(year);
                        maxMinYear.setMinYearISO(iso);
                    }

                    if (year > maxYear) {
                        maxMinYear.setMaxYear(year);
                        maxMinYear.setMaxYearISO(iso);
                    }

                    companyProductYearMap.put(companyProduct, maxMinYear);
                }
            }
        } catch (java.io.FileNotFoundException e) {
            e.printStackTrace();
        } catch (java.io.UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }


    public static class MaxMinYear {
        public int maxYear;
        public int minYear;

        public String minYearISO;
        public String maxYearISO;

        public int getMaxYear() {
            return maxYear;
        }

        public void setMaxYear(int maxYear) {
            this.maxYear = maxYear;
        }

        public int getMinYear() {
            return minYear;
        }

        public void setMinYear(int minYear) {
            this.minYear = minYear;
        }

        public String getMinYearISO() {
            return minYearISO;
        }

        public void setMinYearISO(String minYearISO) {
            this.minYearISO = minYearISO;
        }

        public String getMaxYearISO() {
            return maxYearISO;
        }

        public void setMaxYearISO(String maxYearISO) {
            this.maxYearISO = maxYearISO;
        }

        public MaxMinYear(int maxYear, int minYear, String minYearISO, String maxYearISO) {
            this.maxYear = maxYear;
            this.minYear = minYear;
            this.minYearISO = minYearISO;
            this.maxYearISO = maxYearISO;
        }
    }

}






