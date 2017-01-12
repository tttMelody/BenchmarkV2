package io.kyligence.benchmark.loadtest.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by xiefan on 17-1-3.
 */
public class BenchmarkUtil {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkUtil.class);

    public static String DATE_FORMAT = "yyyy-MM-dd";

    public static long convert2long(String date, String format) {
        try {
            SimpleDateFormat sf = new SimpleDateFormat(format);
            return sf.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0l;
    }

    public static String convert2String(long time, String format) {
        SimpleDateFormat sf = new SimpleDateFormat(format);
        Date date = new Date(time);
        return sf.format(date);
    }

    public static void main(String[] args) {
        String date = convert2String(System.currentTimeMillis(), DATE_FORMAT);
        logger.info(date);
    }
}
