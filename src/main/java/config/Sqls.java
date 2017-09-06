package config;

import java.util.HashMap;
import java.util.Map;

public class Sqls {
    public static final String selectAllColClause = "*";
    public static final String selectOneColClause = "USER_MSISDN";
    public static final String selectTenColClause = "USER_MSISDN, OPPO_MSISDN_REG, OPPO_MSISDN, USER_IMSI, USER_IMEI, EVENT_BEGIN_DATE"
            + ", CALL_DURATION, EVENT_TYPE, USER_BELONG_AREA_CODE, OPPO_BELONG_AREA_CODE";
    public static final String selectTwentyColClause = "USER_MSISDN, OPPO_MSISDN_REG, OPPO_MSISDN, USER_IMSI, USER_IMEI, EVENT_BEGIN_DATE"
            + ", CALL_DURATION, EVENT_TYPE, USER_BELONG_AREA_CODE, OPPO_BELONG_AREA_CODE, USER_CURR_AREA_CODE"
            + ", USER_LAC, USER_CI, CALLTYPE, CALLFLAG, SMS_FLAG, USER_END_LAC, SWITCH_ID, USER_BS_LONGITUDE, USER_BS_LATITUDE";

    public static final String MSISDN_WhereClause = "USER_MSISDN";
    public static final String IMSI_WhereClause = "USER_IMSI";
    public static final String EVENT_BEGIN_DATE_WhereClause = " EVENT_BEGIN_DATE";

    // USER_MSISDN
    public static final String[] max20MISIDNWhereCondition = new String[] { "'13951276335'", "'13949433117'",
            "'13949548435'", "'13950953948'", "'13950714954'", "'13949392534'", "'13950816743'", "'13949592970'",
            "'13954504352'", "'13946522363'", "'13952729111'", "'13949600932'", "'13949232994'", "'13950338997'",
            "'13945610181'", "'13952557867'", "'13948728401'", "'13953117857'", "'13948512093'", "'13950079666'" };
    public static final String[] min20MISIDNWhereCondition = new String[] { "'13940000839'", "'13949999006'",
            "'13940069712'", "'13940002904'", "'13940011311'", "'13940015882'", "'13940025255'", "'13949970306'",
            "'13940055117'", "'13949953135'", "'13940062110'", "'13940085266'", "'13940075893'", "'13940078924'",
            "'13940031149'", "'13940017009'", "'13940037785'", "'13940018913'", "'13940023841'", "'13940024002'" };
    public static final String[] mid20MISIDNWhereCondition = new String[] { "'13951199308'", "'13947957903'",
            "'13949007371'", "'13951241273'", "'13949809053'", "'13952220965'", "'13948228642'", "'13951312456'",
            "'13955224046'", "'13957135410'", "'13947135732'", "'13948781082'", "'13949817817'", "'13949351610'",
            "'13952027317'", "'13950891105'", "'13949140175'", "'13950125011'", "'13957405939'", "'13949195258'" };

    // USER_IMSI
    public static final String[] max20IMSIWhereCondition = new String[] { "'50647144'", "'54116899'", "'52035590'",
            "'50161837'", "'52643016'", "'50287802'", "'48102521'", "'50410766'", "'47055199'", "'49514473'",
            "'46452114'", "'49056638'", "'51110781'", "'52978317'", "'47702640'", "'53499509'", "'49485053'",
            "'47772123'", "'51492291'", "'50773013'" };
    public static final String[] min20IMSIWhereCondition = new String[] { "'47159239'", "'37728293'", "'37304373'",
            "'71762050'", "'62040622'", "'69697450'", "'68347402'", "'35765787'", "'69706557'", "'67402234'",
            "'60073373'", "'65959789'", "'74622306'", "'42871581'", "'68998311'", "'64026284'", "'71535866'",
            "'36714525'", "'66956008'", "'74628942'" };
    public static final String[] mid20IMSIWhereCondition = new String[] { "'56119988'", "'48706803'", "'54532220'",
            "'48374457'", "'52135962'", "'52029702'", "'51631885'", "'56128108'", "'49920274'", "'47925491'",
            "'51810301'", "'52606292'", "'52222699'", "'54841018'", "'51018083'", "'45933539'", "'46320825'",
            "'52472403'", "'50563391'", "'50472237'" };

    // SELECT * FROM CDRTEST_V WHERE EVENT_BEGIN_DATE > '2017-01-01 08:00:00.0' AND EVENT_BEGIN_DATE < '2017-01-01 08:01:00.0' LIMIT 100000
    public static final String[][] dataRangeSQL = new String[][] {
            { "'2017-01-01 08:00:00.0'", "'2017-01-01 08:00:02.0'" },
            { "'2017-01-01 08:00:02.0'", "'2017-01-01 08:00:04.0'" },
            { "'2017-01-01 08:00:04.0'", "'2017-01-01 08:00:06.0'" },
            { "'2017-01-01 08:00:06.0'", "'2017-01-01 08:00:08.0'" },
            { "'2017-01-01 08:00:08.0'", "'2017-01-01 08:00:10.0'" },
            { "'2017-01-01 08:00:10.0'", "'2017-01-01 08:00:12.0'" },
            { "'2017-01-01 08:00:12.0'", "'2017-01-01 08:00:14.0'" },
            { "'2017-01-01 08:00:14.0'", "'2017-01-01 08:00:16.0'" },
            { "'2017-01-01 08:00:16.0'", "'2017-01-01 08:00:18.0'" },
            { "'2017-01-01 08:00:18.0'", "'2017-01-01 08:00:20.0'" },
            { "'2017-01-01 08:00:20.0'", "'2017-01-01 08:00:22.0'" },
            { "'2017-01-01 08:00:22.0'", "'2017-01-01 08:00:24.0'" },
            { "'2017-01-01 08:00:24.0'", "'2017-01-01 08:00:26.0'" },
            { "'2017-01-01 08:00:26.0'", "'2017-01-01 08:00:28.0'" },
            { "'2017-01-01 08:00:28.0'", "'2017-01-01 08:00:30.0'" },
            { "'2017-01-01 08:00:30.0'", "'2017-01-01 08:00:32.0'" },
            { "'2017-01-01 08:00:32.0'", "'2017-01-01 08:00:34.0'" },
            { "'2017-01-01 08:00:34.0'", "'2017-01-01 08:00:36.0'" },
            { "'2017-01-01 08:00:36.0'", "'2017-01-01 08:00:38.0'" },
            { "'2017-01-01 08:00:38.0'", "'2017-01-01 08:00:40.0'" } };

    public static final String LIMIT_10000 = " LIMIT 10000";
    public static final String LIMIT_9999 = " LIMIT 9999";

    public static Map<String, String[]> getDataRangeSQL(String selectClause, String whereClause) {
        Map<String, String[]> result = new HashMap<>();
        String[] sqls = new String[20];
        for (int i = 0; i < 20; i++) {
            sqls[i] = "SELECT " + selectClause + " FROM CDRTEST_V WHERE " + whereClause + " > " + dataRangeSQL[i][0]
                    + " AND " + whereClause + " < " + dataRangeSQL[i][1];
        }
        result.put("Select " + selectClause + " With Date range query", sqls);
        return result;
    }

    public static Map<String, String[]> getSQL(String selectClause, String whereClause) {
        Map<String, String[]> whereClauseConds = new HashMap<>();
        if (whereClause.equals(MSISDN_WhereClause)) {
            whereClauseConds.put("Max", max20MISIDNWhereCondition);
            whereClauseConds.put("Min", min20MISIDNWhereCondition);
            whereClauseConds.put("Mid", mid20MISIDNWhereCondition);
        } else if (whereClause.equals(IMSI_WhereClause)) {
            whereClauseConds.put("Max", max20IMSIWhereCondition);
            whereClauseConds.put("Min", min20IMSIWhereCondition);
            whereClauseConds.put("Mid", mid20IMSIWhereCondition);
        }

        Map<String, String[]> result = new HashMap<>();

        for (String key : whereClauseConds.keySet()) {
            String[] sqls = new String[20];
            for (int i = 0; i < 20; i++) {
                sqls[i] = "SELECT " + selectClause + " FROM CDRTEST_V WHERE " + whereClause + " = "
                        + whereClauseConds.get(key)[i];
            }
            result.put("S_" + selectClause + "_With_" + key + "_" + whereClause, sqls);
        }
        return result;
    }

    public static void main(String[] args) {
        //        for (String max : max20MISIDNWhereCondition) {
        //            System.out.println(selectClause + MSISDN_WhereClause + max + LIMIT_10000);
        //        }
        //        Map<String, String[]> as= Sqls.getSQL(Sqls.selectOneColClause, Sqls.MSISDN_WhereClause);
        Map<String, String[]> as = Sqls.getSQL(Sqls.selectOneColClause, Sqls.IMSI_WhereClause);
        for (String s : as.keySet()) {
            System.out.println();
            System.out.println(s);
            for (String string : as.get(s)) {
                System.out.println(string);
            }
        }
    }
}
